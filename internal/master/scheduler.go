package master

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"
	"mini-spark/internal/common"
	"mini-spark/internal/storage"
)

type Scheduler struct {
	mu           sync.Mutex
	PendingTasks []common.Task
	Registry     *WorkerRegistry
	Store        *storage.JobStore
	NextWorker   int
}

func NewScheduler(r *WorkerRegistry, s *storage.JobStore) *Scheduler {
	return &Scheduler{Registry: r, Store: s, PendingTasks: make([]common.Task, 0)}
}

func (s *Scheduler) SubmitTasks(tasks []common.Task) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.PendingTasks = append(s.PendingTasks, tasks...)
	fmt.Printf("[Scheduler] %d tareas añadidas a la cola.\n", len(tasks))
}

// OnTaskCompleted se llama cuando llega un reporte exitoso
func (s *Scheduler) OnTaskCompleted(report common.TaskReport, jobID, stageID string) {
	// 1. Verificar si el Job existe
	job := s.Store.GetJob(jobID)
	if job == nil { return }

	// 2. Encontrar qué nodo del grafo es esta etapa
	var currentNode common.OperationNode
	var nextNode *common.OperationNode
	
	// Lógica simple secuencial: Graph[0] -> Graph[1] -> ...
	for i, node := range job.Graph {
		if node.ID == stageID {
			currentNode = node
			if i+1 < len(job.Graph) {
				nextNode = &job.Graph[i+1]
			}
			break
		}
	}

	// 3. Verificar si la etapa completa terminó
	if s.Store.CheckStageComplete(stageID, currentNode.Partitions) {
		fmt.Printf("[Scheduler] Etapa %s completada. Planificando siguiente etapa...\n", stageID)
		
		if nextNode != nil {
			s.createNextStageTasks(job, *nextNode, stageID)
		} else {
			fmt.Printf("[Scheduler] JOB %s FINALIZADO CON ÉXITO.\n", jobID)
		}
	}
}

// createNextStageTasks genera las tareas de REDUCE basándose en los resultados de MAP
func (s *Scheduler) createNextStageTasks(job *common.JobRequest, nextNode common.OperationNode, prevStageID string) {
	prevReports := s.Store.GetStageResults(prevStageID)
	var newTasks []common.Task

	// Para cada partición de la NUEVA etapa (ej. 2 Reducers)
	for i := 0; i < nextNode.Partitions; i++ {
		// Construir el ShuffleMap: ¿Dónde están los datos para la partición 'i'?
		shuffleMap := make(map[string]string)
		
		for _, rep := range prevReports {
			for _, meta := range rep.ShuffleOutput {
				// Si este archivo pertenece a la partición 'i'
				if meta.PartitionKey == i {
					// Construimos la URL para que el Worker descargue el archivo
					// Formato: http://WorkerIP:Port/shuffle?path=/ruta/al/archivo
					url := fmt.Sprintf("http://%s/shuffle?path=%s", rep.WorkerID, meta.Path)
					// Usamos una clave única para el mapa
					key := fmt.Sprintf("%s-%d", rep.WorkerID, len(shuffleMap))
					shuffleMap[key] = url
				}
			}
		}

		newTask := common.Task{
			TaskID:    fmt.Sprintf("%s-%s-%d", job.JobID, nextNode.ID, i),
			JobID:     job.JobID,
			StageID:   nextNode.ID,
			Operation: nextNode,
			InputPartition: common.TaskInput{
				SourceType: common.SourceTypeShuffle, // Indicar que debe descargar
				ShuffleMap: shuffleMap,
			},
			OutputTarget: common.TaskOutput{
				Type:       common.OutputTypeLocalSpill, // Salida final del Reduce
				Path:       fmt.Sprintf("%s/%s", job.OutputPath, nextNode.ID),
				Partitions: 1, 
			},
		}
		newTasks = append(newTasks, newTask)
	}
	
	s.SubmitTasks(newTasks)
}

func (s *Scheduler) Start() {
	go func() {
		for {
			time.Sleep(100 * time.Millisecond)
			s.scheduleLoop()
		}
	}()
}

func (s *Scheduler) scheduleLoop() {
	s.mu.Lock()
	if len(s.PendingTasks) == 0 {
		s.mu.Unlock()
		return
	}
	
	workers := s.Registry.GetIdleWorkers()
	if len(workers) == 0 {
		s.mu.Unlock()
		return
	}

	task := s.PendingTasks[0]
	worker := workers[s.NextWorker%len(workers)]
	s.NextWorker++

	s.PendingTasks = s.PendingTasks[1:]
	s.mu.Unlock()

	if err := s.assignTask(task, worker); err != nil {
		fmt.Printf("[Scheduler] Fallo asignando %s a %s. Reintentando.\n", task.TaskID, worker.Address)
		s.SubmitTasks([]common.Task{task})
	} else {
		fmt.Printf("[Scheduler] Asignada %s -> %s\n", task.TaskID, worker.Address)
	}
}

func (s *Scheduler) assignTask(t common.Task, w common.Heartbeat) error {
	data, _ := json.Marshal(t)
	resp, err := http.Post("http://"+w.Address+"/tasks", "application/json", bytes.NewBuffer(data))
	if err != nil { return err }
	defer resp.Body.Close()
	if resp.StatusCode != 200 { return fmt.Errorf("status %d", resp.StatusCode) }
	return nil
}