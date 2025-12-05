package master

import (
	//"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"mini-spark/internal/common"
	"mini-spark/internal/storage"
)

// Constante para el número máximo de reintentos
const MaxTaskRetries = 3 

// Función de ayuda para crear un JobRequest de prueba
func createTestJob(jobID string) common.JobRequest {
	return common.JobRequest{
		JobID:      jobID,
		NumPartitions: 2,
		DAG: common.DAG{
			Nodes: []common.OperationNode{
				{ID: "stage-map", Type: common.OpTypeMap, UDFName: "map_wc", NumPartitions: 2},
				{ID: "stage-reduce", Type: common.OpTypeReduceByKey, UDFName: "reduce_sum", NumPartitions: 2},
			},
			Edges: [][]string{{"stage-map", "stage-reduce"}},
		},
	}
}

func TestScheduler_StageTransitionAndFaultTolerance(t *testing.T) {
	// Reemplazamos MaxTaskRetries de la constante common si fuera necesario, o lo asumimos en 3.
	
	store := storage.NewJobStore()
	registry := NewWorkerRegistry()
	scheduler := NewScheduler(registry, store)

	// Simular el worker (Necesario para el Heartbeat)
	workerHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	workerServer := httptest.NewServer(workerHandler)
	defer workerServer.Close()
	workerAddress := workerServer.Listener.Addr().String()

	// 1. Setup inicial: Worker y Job
	workerID := "worker-1"
	registry.UpdateHeartbeat(common.Heartbeat{
		WorkerID: workerID,
		Address:  workerAddress,
		Status:   common.WorkerStatusIdle,
	})

	jobID := "job-test-123"
	job := createTestJob(jobID)
	store.CreateJob(&job)

	// 2. Transición de Etapa (Stage 1: MAP)
	t.Run("StageTransition_MapToReduce", func(t *testing.T) {
		// Asignar el job (encoñará las 2 tareas MAP)
		scheduler.SubmitJob(&job)
		time.Sleep(10 * time.Millisecond) // Dejar que el scheduler encole

		// Simular éxito de las 2 tareas MAP
		mapReports := []common.TaskReport{
			{
				TaskID: "job-test-123-stage-map-0", JobID: jobID, StageID: "stage-map", Status: common.TaskStatusSuccess, WorkerID: workerID,
				ShuffleOutput: []common.ShuffleMeta{{PartitionKey: 0, Path: "/tmp/map-0_p0", Size: 10}},
			},
			{
				TaskID: "job-test-123-stage-map-1", JobID: jobID, StageID: "stage-map", Status: common.TaskStatusSuccess, WorkerID: workerID,
				ShuffleOutput: []common.ShuffleMeta{{PartitionKey: 0, Path: "/tmp/map-1_p0", Size: 10}},
			},
		}

		// Marcar tareas como completadas
		for _, rep := range mapReports {
			store.AddTaskReport(jobID, "stage-map", rep)
			scheduler.HandleTaskCompletion(rep)
		}

		// Verificar que la siguiente etapa (REDUCE) fue encolada
		scheduler.mu.Lock()
		numReduceTasks := len(scheduler.PendingTasks)
		scheduler.mu.Unlock()
		
		if numReduceTasks != 2 {
			t.Fatalf("Esperaba 2 tareas de Reduce encoladas, obtuvo %d", numReduceTasks)
		}
	})

	// 3. Tolerancia a Fallos (Worker Muerto)
	t.Run("FaultTolerance_DeadWorker", func(t *testing.T) {
		// Simular una tarea en ejecución (tomamos la primera tarea de Reduce)
		scheduler.mu.Lock()
		runningTask := scheduler.PendingTasks[0] // Tomar la primera tarea de Reduce
		scheduler.PendingTasks = scheduler.PendingTasks[1:] // Remover de Pending
		scheduler.RunningTasks[runningTask.TaskID] = runningTask
		scheduler.AssignedWorker[runningTask.TaskID] = workerID
		scheduler.mu.Unlock()

		// Simular la muerte del worker
		registry.mu.Lock()
		registry.workers[workerID] = common.Heartbeat{
			WorkerID: workerID,
			Address:  workerAddress,
			Status:   common.WorkerStatusIdle,
			LastHeartbeat: time.Now().Unix() - WorkerTimeoutSeconds - 1, // Expirado
		}
		registry.mu.Unlock()

		// Forzar el loop de control
		scheduler.ControlLoop()
		time.Sleep(10 * time.Millisecond) // Dejar que el loop procese

		// Verificar que la tarea fue re-encolada
		scheduler.mu.Lock()
		isRequeued := false
		for _, task := range scheduler.PendingTasks {
			if task.TaskID == runningTask.TaskID {
				isRequeued = true
				if task.RetryCount != 0 { t.Errorf("La tarea re-encolada no debe aumentar el RetryCount") }
			}
		}
		scheduler.mu.Unlock()

		if !isRequeued {
			t.Errorf("La tarea no fue re-encolada después de la muerte del worker.")
		}
	})

	// 4. Tolerancia a Fallos (Fallo de Tarea con Reintento)
	t.Run("FaultTolerance_TaskFailure", func(t *testing.T) {
		failTask := common.Task{TaskID: "fail-task-0", JobID: jobID, StageID: "stage-map", Operation: job.DAG.Nodes[0], RetryCount: 0}
		scheduler.RunningTasks[failTask.TaskID] = failTask

		// Fallo #1 (RetryCount=1)
		scheduler.HandleTaskFailure(failTask, "Fallo #1")
		
		// Simular dos fallos más hasta abortar (asumiendo MaxTaskRetries = 3)
		requeuedTask := scheduler.PendingTasks[0]
		scheduler.HandleTaskFailure(requeuedTask, "Fallo #2") // RetryCount=2
		scheduler.HandleTaskFailure(requeuedTask, "Fallo #3") // RetryCount=3

		// Fallo definitivo (el cuarto intento)
		finalTask := scheduler.PendingTasks[0] // Tarea con RetryCount=3
		scheduler.HandleTaskFailure(finalTask, "Fallo #4 (Final)") 


		finalStatus := store.GetJob(jobID).Status
		if finalStatus != common.JobStatusFailed {
			t.Errorf("Job no abortado después de MaxTaskRetries. Estado: %s", finalStatus)
		}
	})
}