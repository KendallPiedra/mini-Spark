package master

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"
	"my-mini-spark/internal/common" 
)

// gestiona la asignacion de tareas a workers
type Scheduler struct {
	mu           sync.Mutex
	pendingTasks []common.Task
	workerRegistry *WorkerRegistry // Necesita acceso a los workers vivos
	nextWorkerIdx  int // Para Round-Robin
}

func NewScheduler(registry *WorkerRegistry) *Scheduler {
	return &Scheduler{
		workerRegistry: registry,
		pendingTasks:   make([]common.Task, 0),
	}
}

// añade un lote de tareas a la cola
func (s *Scheduler) EnqueueTasks(tasks []common.Task) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.pendingTasks = append(s.pendingTasks, tasks...)
	// !!! En una version mas avanzada, aqui se notificaria a un Goroutine de Planificacion
}

// Run (Goroutine de Planificacion)
// Esta funcion se ejecutaria en un Goroutine dedicado del Master
func (s *Scheduler) Run() {
	for {
		s.mu.Lock()
		if len(s.pendingTasks) == 0 {
			s.mu.Unlock()
			continue //continua iterando si no hay tareas
		}

		// 1. Obtiene Worker disponible (Round-Robin)
		worker, err := s.getNextWorker()
		if err != nil {
			// No hay workers disponibles. Esperar y reintentar.
			s.mu.Unlock()
			continue
		}

		// 2. Tomar la primera tarea de la cola
		task := s.pendingTasks[0]
		s.pendingTasks = s.pendingTasks[1:] // pop
		s.mu.Unlock()

		// 3. Asignar la tarea al Worker (via HTTP)
		err = s.assignTask(task, worker)
		if err != nil {
			// Si falla la asignacion, la tarea vuelve a la cola.
			s.mu.Lock()
			s.pendingTasks = append(s.pendingTasks, task) 
			s.mu.Unlock()
			fmt.Printf("[Scheduler] Error asignando tarea %s a %s: %v\n", task.TaskID, worker.Address, err)
			continue
		}

		// !!! 4. (SIMULADO) Actualizar estado en SQLite a RUNNING
		// *Aqui iría la lógica de persistencia: storage.UpdateTaskStatus(task.TaskID, "RUNNING")*
		fmt.Printf("[Scheduler] Tarea %s asignada exitosamente a Worker %s\n", task.TaskID, worker.WorkerID)
	}
}

// Selecciona el siguiente Worker disponible usando Round-Robin
func (s *Scheduler) getNextWorker() (*common.Heartbeat, error) {
	workers := s.workerRegistry.GetActiveWorkers()
	if len(workers) == 0 {
		return nil, fmt.Errorf("no hay workers activos disponibles")
	}

	// Usamos nextWorkerIdx para seleccionar cíclicamente
	worker := workers[s.nextWorkerIdx%len(workers)]// logica de modulo para ciclar
	s.nextWorkerIdx++ // Mueve el indice para el siguiente ciclo
	return worker, nil
}

// assignTask llama al endpoint POST /tasks del Worker
func (s *Scheduler) assignTask(task common.Task, worker *common.Heartbeat) error {
	taskJSON, err := json.Marshal(task)
	if err != nil {
		return err
	}

	url := fmt.Sprintf("http://%s/tasks", worker.Address)
	resp, err := http.Post(url, "application/json", bytes.NewBuffer(taskJSON))
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("worker %s devolvio status %d: %s", worker.WorkerID, resp.StatusCode, string(body))
	}

	return nil
}