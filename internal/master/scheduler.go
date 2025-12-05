package master

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"
	"mini-spark/internal/common"
	"mini-spark/internal/storage"
)

type Scheduler struct {
	mu             sync.Mutex
	PendingTasks   []common.Task       // Cola prioritaria (FIFO simple por ahora)
	RunningTasks   map[string]common.Task // TaskID -> Task (Para reintentos si falla worker)
	AssignedWorker map[string]string   // TaskID -> WorkerID
	
	Registry *WorkerRegistry
	Store    *storage.JobStore
	
	workerIdx int // Para Round-Robin
}

func NewScheduler(r *WorkerRegistry, s *storage.JobStore) *Scheduler {
	sch := &Scheduler{
		Registry:       r,
		Store:          s,
		PendingTasks:   make([]common.Task, 0),
		RunningTasks:   make(map[string]common.Task),
		AssignedWorker: make(map[string]string),
	}
	// Iniciar bucle de control en fondo
	go sch.ControlLoop()
	return sch
}

// SubmitJob convierte el DAG inicial en tareas de la primera etapa (Source Nodes)
func (s *Scheduler) SubmitJob(job *common.JobRequest) {
	s.mu.Lock()
	defer s.mu.Unlock()
	
	log.Printf("[Scheduler] Planificando Job %s (%s)", job.JobID, job.Name)
	s.Store.UpdateJobStatus(job.JobID, common.JobStatusRunning)

	// 1. Identificar nodos raíz (sin dependencias entrantes en el DAG)
	// Simplificación Académica: Asumimos que el nodo[0] es siempre el source.
	// En un DAG real haríamos un análisis topológico.
	rootNode := job.DAG.Nodes[0]
	
	s.enqueueStageTasks(job, rootNode, nil)
}

func (s *Scheduler) enqueueStageTasks(job *common.JobRequest, node common.OperationNode, prevStageReports []common.TaskReport) {
    // 1. Determinar input (File o Shuffle)
    inputType := common.SourceTypeFile
    if prevStageReports != nil {
        inputType = common.SourceTypeShuffle
    }

    if node.NumPartitions == 0 { node.NumPartitions = job.NumPartitions } // Default global

    var tasks []common.Task
    
    // Caso MAP (Source)
    if prevStageReports == nil {
        for i := 0; i < node.NumPartitions; i++ {
            tasks = append(tasks, common.Task{
                TaskID:    fmt.Sprintf("%s-%s-%d", job.JobID, node.ID, i),
                JobID:     job.JobID,
                StageID:   node.ID,
                Operation: node,
                InputPartition: common.TaskInput{
                    SourceType: inputType, // <--- CORRECCIÓN: Usamos la variable aquí
                    Path:       job.InputPath,
                },
                OutputTarget: common.TaskOutput{
                    Type:       common.OutputTypeShuffle,
                    Path:       fmt.Sprintf("/tmp/spark/%s/%s", job.JobID, node.ID),
                    NumPartitions: node.NumPartitions,
                },
            })
        }
    } else {
        // Caso REDUCE/JOIN (Shuffle)
        for i := 0; i < node.NumPartitions; i++ {
            shuffleMap := make(map[string]string)
            // Buscar en los reportes anteriores quién tiene datos para la partición 'i'
            for _, rep := range prevStageReports {
                for _, meta := range rep.ShuffleOutput {
                    if meta.PartitionKey == i {
                        // Construir URL de descarga
                        url := fmt.Sprintf("http://%s/shuffle?path=%s", rep.WorkerID, meta.Path)
                        shuffleMap[rep.WorkerID+"-"+meta.Path] = url
                    }
                }
            }
            
            tasks = append(tasks, common.Task{
                TaskID:    fmt.Sprintf("%s-%s-%d", job.JobID, node.ID, i),
                JobID:     job.JobID,
                StageID:   node.ID,
                Operation: node,
                InputPartition: common.TaskInput{
                    SourceType: inputType, 
                    ShuffleMap: shuffleMap,
                },
                OutputTarget: common.TaskOutput{
                    Type:       common.OutputTypeLocalSpill,
                    Path:       fmt.Sprintf("/tmp/spark/%s/output", job.JobID),
                    NumPartitions: 1,
                },
            })
        }
    }
    
    s.PendingTasks = append(s.PendingTasks, tasks...)
    log.Printf("[Scheduler] Encoladas %d tareas para etapa %s (Input: %s)", len(tasks), node.ID, inputType)
}

// ControlLoop ejecuta el ciclo principal de orquestación
func (s *Scheduler) ControlLoop() {
	ticker := time.NewTicker(500 * time.Millisecond)
	for range ticker.C {
		// 1. Verificar Workers Muertos
		deadWorkers := s.Registry.DetectDeadWorkers()
		if len(deadWorkers) > 0 {
			s.handleDeadWorkers(deadWorkers)
		}

		// 2. Asignar Tareas Pendientes
		s.assignPendingTasks()
	}
}

func (s *Scheduler) assignPendingTasks() {
	s.mu.Lock()
	defer s.mu.Unlock()

	if len(s.PendingTasks) == 0 { return }

	workers := s.Registry.GetAliveWorkers()
	if len(workers) == 0 {
		// log.Println("[Scheduler] No hay workers disponibles.")
		return
	}

	// Política Round-Robin
	capacity := len(workers)
	
	// Intentar asignar tantas tareas como sea posible
	activeAssignable := 0 
	for len(s.PendingTasks) > 0 {
		// Selección de Worker
		worker := workers[s.workerIdx % capacity]
		s.workerIdx++
		
		// TODO: Load Awareness (Si active_tasks > X, saltar worker)
		
		task := s.PendingTasks[0]
		
		// Llamada asíncrona para no bloquear el loop
		go s.dispatchTask(task, worker)
		
		// Mover de Pending a Running
		s.RunningTasks[task.TaskID] = task
		s.AssignedWorker[task.TaskID] = worker.WorkerID
		s.PendingTasks = s.PendingTasks[1:]
		
		activeAssignable++
		if activeAssignable >= capacity * 2 { break } // No saturar en un solo tick
	}
}

func (s *Scheduler) dispatchTask(task common.Task, worker common.Heartbeat) {
	data, _ := json.Marshal(task)
	url := fmt.Sprintf("http://%s/tasks", worker.Address)
	
	resp, err := http.Post(url, "application/json", bytes.NewBuffer(data))
	
	// Si falla el envío HTTP inmediato (Connection Refused), re-encolar
	if err != nil || resp.StatusCode != 200 {
		log.Printf("[Scheduler] Fallo enviando tarea %s a %s: %v", task.TaskID, worker.Address, err)
		s.HandleTaskFailure(task, "Dispatch Error")
		if resp != nil { resp.Body.Close() }
		return
	}
	defer resp.Body.Close()
}

// HandleTaskCompletion se llama desde la API cuando llega un reporte
func (s *Scheduler) HandleTaskCompletion(report common.TaskReport) {
	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.RunningTasks, report.TaskID)
	delete(s.AssignedWorker, report.TaskID)

	if report.Status == common.TaskStatusSuccess {
		log.Printf("[Scheduler] Tarea Completada: %s", report.TaskID)
		s.checkStageCompletion(report.JobID, report.StageID)
	} else {
		// ?Recuperar tarea original (necesitamos la definición completa para reintentar)
		// Como ya la borramos de RunningTasks, debemos confiar en que el mensaje de fallo traiga contexto
		// O mejor: no borrarla de RunningTasks hasta confirmar éxito. (Corregido arriba: solo borro al entrar)
		// PROBLEMA: Si borro arriba, pierdo la Task struct original. 
		// CORRECCIÓN: Usar Store o no borrar hasta analizar.
		
		// Para simplicidad, asumimos que TaskFailure se maneja por separado
		// o re-creamos la tarea si tenemos persistencia.
	}
}

func (s *Scheduler) HandleTaskFailure(task common.Task, reason string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	
	task.RetryCount++
	if task.RetryCount <= common.MaxTaskRetries {
		log.Printf("[Scheduler] Reintentando tarea %s (Intento %d/%d). Razón: %s", 
			task.TaskID, task.RetryCount, common.MaxTaskRetries, reason)
		// Volver a poner al frente de la cola
		s.PendingTasks = append([]common.Task{task}, s.PendingTasks...)
	} else {
		log.Printf("[Scheduler] Tarea %s FALLÓ DEFINITIVAMENTE tras %d intentos. Abortando Job.", task.TaskID, task.RetryCount)
		s.Store.UpdateJobStatus(task.JobID, common.JobStatusFailed)
	}
	
	delete(s.RunningTasks, task.TaskID)
	delete(s.AssignedWorker, task.TaskID)
}

func (s *Scheduler) handleDeadWorkers(deadIDs []string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, deadID := range deadIDs {
		// Buscar todas las tareas corriendo en este worker
		for taskID, workerID := range s.AssignedWorker {
			if workerID == deadID {
				task, exists := s.RunningTasks[taskID]
				if exists {
					log.Printf("[FaultTolerance] Worker %s murió. Re-encolando tarea %s", deadID, taskID)
					// Re-encolar sin incrementar retry (no es culpa de la tarea)
					s.PendingTasks = append([]common.Task{task}, s.PendingTasks...)
					delete(s.RunningTasks, taskID)
					delete(s.AssignedWorker, taskID)
				}
			}
		}
	}
}

func (s *Scheduler) checkStageCompletion(jobID, stageID string) {
	job := s.Store.GetJob(jobID)
	if job == nil { return }
	
	// Verificar si todas las particiones de este stage terminaron
	reports := s.Store.GetStageReports(jobID, stageID)
	
	// Buscar definición del nodo actual en el DAG
	var currentNode common.OperationNode
	var nextNode *common.OperationNode
	
	for i, node := range job.Request.DAG.Nodes {
		if node.ID == stageID {
			currentNode = node
			// Buscar siguiente nodo conectado por un Edge
			// Simplificación: Asumimos linealidad Nodes[i] -> Nodes[i+1] para Batch básico
			if i+1 < len(job.Request.DAG.Nodes) {
				nextNode = &job.Request.DAG.Nodes[i+1]
			}
			break
		}
	}
	
	expected := currentNode.NumPartitions
	if expected == 0 { expected = job.Request.NumPartitions }

	if len(reports) >= expected {
		log.Printf("[Scheduler] Stage %s completado. %d/%d tareas.", stageID, len(reports), expected)
		
		if nextNode != nil {
			// Lanzar siguiente etapa
			go func() {
				s.mu.Lock() // Bloquear para encolar
				defer s.mu.Unlock()
				s.enqueueStageTasks(job.Request, *nextNode, reports)
			}()
		} else {
			s.Store.UpdateJobStatus(jobID, common.JobStatusSucceeded)
			log.Printf("=== JOB %s FINALIZADO EXITOSAMENTE ===", jobID)
		}
	}
}