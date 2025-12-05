package master

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"

	"mini-spark/internal/common"
)

// WorkerRegistry almacena el estado de los workers en orden determinista.
type WorkerRegistry struct {
	Mu      sync.RWMutex
	workers map[string]common.Heartbeat
	order   []string // NUEVO: Slice para mantener el orden determinista
}

func NewWorkerRegistry() *WorkerRegistry {
	return &WorkerRegistry{
		workers: make(map[string]common.Heartbeat),
		order:   make([]string, 0),
	}
}

// RegisterWorker añade o actualiza el worker, y garantiza que su ID esté en la lista de orden.
func (r *WorkerRegistry) RegisterWorker(w common.Heartbeat) {
	r.Mu.Lock()
	defer r.Mu.Unlock()
	
	// Si el worker es nuevo, lo añadimos al final del slice 'order'
	if _, exists := r.workers[w.WorkerID]; !exists {
		r.order = append(r.order, w.WorkerID)
	}
	r.workers[w.WorkerID] = w // Actualizamos el estado del worker
}

// GetActiveWorkers devuelve una lista de workers disponibles en ORDEN determinista.
func (r *WorkerRegistry) GetActiveWorkers() []common.Heartbeat {
	r.Mu.RLock()
	defer r.Mu.RUnlock()
	
	active := make([]common.Heartbeat, 0)
	
	// Iteramos sobre el slice 'order' (ordenado) en lugar de sobre el mapa (aleatorio)
	for _, id := range r.order {
		w := r.workers[id]
		if w.Status == common.WorkerStatusIdle {
			active = append(active, w)
		}
	}
	return active
}


// Scheduler y sus métodos (NewScheduler, EnqueueTasks, getNextWorker, Run, assignTask)
// El resto del código del Scheduler permanece igual, pero su campo WorkerRegistry
// ahora usa el orden garantizado en GetActiveWorkers.
type Scheduler struct {
	Mu           sync.Mutex
	PendingTasks []common.Task
	WorkerRegistry *WorkerRegistry
	NextWorkerIdx  int
}

func NewScheduler(registry *WorkerRegistry) *Scheduler {
	return &Scheduler{
		WorkerRegistry: registry,
		PendingTasks:   make([]common.Task, 0),
	}
}

func (s *Scheduler) EnqueueTasks(tasks []common.Task) {
	s.Mu.Lock()
	defer s.Mu.Unlock()
	s.PendingTasks = append(s.PendingTasks, tasks...)
}

func (s *Scheduler) Run() {
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()

	for range ticker.C {
		s.Mu.Lock()
		if len(s.PendingTasks) == 0 {
			s.Mu.Unlock()
			continue
		}

		worker, err := s.getNextWorker()
		if err != nil {
			s.Mu.Unlock()
			continue
		}

		task := s.PendingTasks[0] 
		s.PendingTasks = s.PendingTasks[1:] 
		s.Mu.Unlock() 

		err = s.assignTask(task, worker)
		if err != nil {
			s.Mu.Lock()
			// Insertar al principio para reintento inmediato
			s.PendingTasks = append([]common.Task{task}, s.PendingTasks...) 
			s.Mu.Unlock()
			fmt.Printf("[Scheduler] Error asignando tarea %s a %s: %v. Reintentando...\n", task.TaskID, worker.Address, err)
			continue
		}

		fmt.Printf("[Scheduler] Tarea %s asignada exitosamente a Worker %s\n", task.TaskID, worker.WorkerID)
	}
}

func (s *Scheduler) getNextWorker() (*common.Heartbeat, error) {
	workers := s.WorkerRegistry.GetActiveWorkers()
	if len(workers) == 0 {
		return nil, fmt.Errorf("no hay workers activos disponibles")
	}

	worker := workers[s.NextWorkerIdx%len(workers)]
	s.NextWorkerIdx++ 
	return &worker, nil
}

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