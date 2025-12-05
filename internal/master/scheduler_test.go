package master

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"mini-spark/internal/common"
	// Usamos package master para White-Box Testing
)

// Helper para crear un worker y registrarlo
func AddWorkerToRegistry(r *WorkerRegistry, id, address string) {
    r.RegisterWorker(common.Heartbeat{
		WorkerID: id,
		Address: address,
		Status: common.WorkerStatusIdle,
		LastHeartbeat: time.Now().Unix(),
	})
}

func createTestTask(id string) common.Task {
	return common.Task{TaskID: id, JobID: "job-test"}
}

// --- Test de Planificación de Round-Robin (Lógica interna) ---

func TestSchedulerRoundRobin(t *testing.T) {
	registry := NewWorkerRegistry()
	
	// El orden de registro determina el orden de Round-Robin
	AddWorkerToRegistry(registry, "w1", "w1_addr")
	AddWorkerToRegistry(registry, "w2", "w2_addr")
	AddWorkerToRegistry(registry, "w3", "w3_addr")
	
	s := NewScheduler(registry) 

	// Obtenemos los Workers en el orden determinista garantizado
	workers := registry.GetActiveWorkers()
	if len(workers) != 3 {
		t.Fatalf("Error de setup: se esperaban 3 workers activos, obtuve %d", len(workers))
	}

	tests := []struct {
		name           string
		iteration      int
		expectedWorker common.Heartbeat
	}{
		// **ESPERAMOS W1, W2, W3, W1**
		{name: "Round-Robin 1", iteration: 1, expectedWorker: workers[0]}, // w1
		{name: "Round-Robin 2", iteration: 2, expectedWorker: workers[1]}, // w2
		{name: "Round-Robin 3", iteration: 3, expectedWorker: workers[2]}, // w3
		{name: "Round-Robin 4 (Ciclo)", iteration: 4, expectedWorker: workers[0]}, // w1
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s.Mu.Lock() 
			worker, err := s.getNextWorker() // Accediendo al método privado
			s.Mu.Unlock()
            
            if err != nil {
                t.Fatalf("getNextWorker falló inesperadamente: %v", err)
            }

			if worker.WorkerID != tt.expectedWorker.WorkerID {
				t.Errorf("Iteración %d falló. Esperaba Worker %s, Obtenido %s", tt.iteration, tt.expectedWorker.WorkerID, worker.WorkerID)
			}
		})
	}
}

// --- Test de Asignación y Reintento (Simulación de la Red) ---

func TestSchedulerAssignmentAndRetry(t *testing.T) {
	registry := NewWorkerRegistry()
	AddWorkerToRegistry(registry, "w1", "w1_addr")
	s := NewScheduler(registry)

	// Iniciar el Scheduler en una goroutine
	go s.Run()
	
	// --- Caso 1: Asignación Exitosa ---
	t.Run("Asignacion Exitosa", func(t *testing.T) {
		successTask := createTestTask("task-SUCCESS")
		s.EnqueueTasks([]common.Task{successTask})

		successServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK) 
		}))
		defer successServer.Close()
		
		// Apuntamos el worker al servidor que responde 200 OK
		registry.RegisterWorker(common.Heartbeat{
			WorkerID: "w1", 
			Address: successServer.Listener.Addr().String(),
			Status: common.WorkerStatusIdle,
		})
		
		time.Sleep(100 * time.Millisecond) 

		s.Mu.Lock()
		remaining := len(s.PendingTasks) 
		s.Mu.Unlock()

		if remaining != 0 {
			t.Errorf("Asignación Exitosa Falló: Esperaba cola vacía, quedan %d tareas", remaining)
		}
	})

	// --- Caso 2: Asignación Fallida (Verificar Reintento) ---
	t.Run("Asignacion Fallida y Reintento", func(t *testing.T) {
		failTask := createTestTask("task-FAIL")
		s.EnqueueTasks([]common.Task{failTask})

		// Servidor de prueba que siempre devuelve 500
		failServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusInternalServerError) 
		}))
		defer failServer.Close()
		
		// Apuntamos el worker al servidor que falla
		registry.RegisterWorker(common.Heartbeat{
			WorkerID: "w1", 
			Address: failServer.Listener.Addr().String(),
			Status: common.WorkerStatusIdle,
		})
		
		time.Sleep(150 * time.Millisecond) // Damos tiempo suficiente para que falle y reencole

		s.Mu.Lock()
		remaining := len(s.PendingTasks) 
		requeuedTaskID := ""
		if remaining > 0 {
			requeuedTaskID = s.PendingTasks[0].TaskID 
		}
		s.Mu.Unlock()

		if remaining != 1 {
			t.Fatalf("Reintento Falló: Esperaba 1 tarea en cola, obtuve %d", remaining)
		}
		if requeuedTaskID != failTask.TaskID {
			t.Errorf("Reintento Falló: La tarea que regresó no es la original. Esperaba %s, obtuve %s", failTask.TaskID, requeuedTaskID)
		}
	})
}