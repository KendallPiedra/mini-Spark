package master
/*
import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"mini-spark/internal/common"
	"mini-spark/internal/storage"
)

// ==========================================================
// SETUP Y UTILITIES
// ==========================================================

func setupMasterServer(t *testing.T) *MasterServer {
	// Inicialización de los componentes internos del Master
	store := storage.NewJobStore()
	registry := NewWorkerRegistry()
	scheduler := NewScheduler(registry, store)
	
	// Detener el loop de scheduling para evitar que interfiera en las pruebas
	// (Asumimos que el ControlLoop se detiene correctamente al finalizar el proceso de testeo)

	return &MasterServer{
		Scheduler: scheduler,
		Registry:  registry,
		Store:     store,
	}
}

// Helper para crear un JobRequest básico
func createValidJobRequest() common.JobRequest {
	return common.JobRequest{
		Name:       "TestJob",
		InputPath:  "/data/input.txt",
		NumPartitions: 2,
		DAG: common.DAG{
			Nodes: []common.OperationNode{
				{ID: "m1", Type: common.OpTypeMap, UDFName: "wc", NumPartitions: 2},
			},
			Edges: [][]string{},
		},
	}
}

// ==========================================================
// TESTS DE ENDPOINTS
// ==========================================================

func TestMasterAPI_HandleSubmitJob(t *testing.T) {
	server := setupMasterServer(t)
	validReq := createValidJobRequest()
	
	tests := []struct {
		name       string
		method     string
		body       common.JobRequest
		expectCode int
		expectTasks int
	}{
		{
			name:       "Success_ValidJob",
			method:     "POST",
			body:       validReq,
			expectCode: http.StatusOK,
			expectTasks: 2, // 2 particiones en el nodo MAP inicial
		},
		{
			name:       "Failure_WrongMethod",
			method:     "GET",
			body:       validReq,
			expectCode: http.StatusMethodNotAllowed,
			expectTasks: 0,
		},
		{
			name:       "Failure_InvalidJSON",
			method:     "POST",
			body:       common.JobRequest{}, // Cuerpo vacío para simular JSON inválido (aunque técnicamente es válido)
			expectCode: http.StatusBadRequest, // Esperamos que falle si el JSON es demasiado mínimo o si hay un error de decodificación
			expectTasks: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bodyJSON, _ := json.Marshal(tt.body)
			
			req := httptest.NewRequest(tt.method, "/api/v1/jobs", bytes.NewBuffer(bodyJSON))
			rr := httptest.NewRecorder()

			server.HandleSubmitJob(rr, req)

			if rr.Code != tt.expectCode {
				t.Fatalf("Esperaba código %d, obtuvo %d. Body: %s", tt.expectCode, rr.Code, rr.Body.String())
			}
			
			// Verificar la acción interna (solo para el caso de éxito)
			if tt.expectCode == http.StatusOK {
				server.Scheduler.mu.Lock()
				numTasks := len(server.Scheduler.PendingTasks)
				server.Scheduler.mu.Unlock()

				if numTasks != tt.expectTasks {
					t.Errorf("Scheduler no encoló tareas correctamente. Esperaba %d, obtuvo %d", tt.expectTasks, numTasks)
				}
				// Limpiar cola para la siguiente prueba
				server.Scheduler.PendingTasks = nil
			}
		})
	}
}

func TestMasterAPI_HandleGetJob(t *testing.T) {
	server := setupMasterServer(t)
	jobID := "1234-5678"
	// Simular un job guardado
	server.Store.CreateJob(&common.JobRequest{JobID: jobID, Name: "LookupTest"})
	
	tests := []struct {
		name       string
		path       string
		expectCode int
		expectName string
	}{
		{
			name:       "Success_Found",
			path:       "/api/v1/jobs/" + jobID,
			expectCode: http.StatusOK,
			expectName: "LookupTest",
		},
		{
			name:       "Failure_NotFound",
			path:       "/api/v1/jobs/9999-9999",
			expectCode: http.StatusNotFound,
			expectName: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest("GET", tt.path, nil)
			rr := httptest.NewRecorder()

			server.HandleGetJob(rr, req)

			if rr.Code != tt.expectCode {
				t.Fatalf("Esperaba código %d, obtuvo %d", tt.expectCode, rr.Code)
			}

			if tt.expectCode == http.StatusOK {
				var jobState storage.JobState
				json.Unmarshal(rr.Body.Bytes(), &jobState)
				if jobState.Request.Name != tt.expectName {
					t.Errorf("Nombre de job incorrecto. Esperaba %s, obtuvo %s", tt.expectName, jobState.Request.Name)
				}
			}
		})
	}
}

func TestMasterAPI_HandleHeartbeat(t *testing.T) {
	server := setupMasterServer(t)
	workerID := "worker-hb-test"

	hb := common.Heartbeat{WorkerID: workerID, Address: "localhost:9000", Status: common.WorkerStatusIdle}
	hbJSON, _ := json.Marshal(hb)

	req := httptest.NewRequest("POST", "/heartbeat", bytes.NewBuffer(hbJSON))
	rr := httptest.NewRecorder()

	server.HandleHeartbeat(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("Heartbeat falló. Código: %d", rr.Code)
	}

	// Verificar que el Worker fue registrado (a través de GetAliveWorkers)
	alive := server.Registry.GetAliveWorkers()
	found := false
	for _, w := range alive {
		if w.WorkerID == workerID {
			found = true
			break
		}
	}
	if !found {
		t.Error("Worker no fue registrado después del Heartbeat.")
	}
}

func TestMasterAPI_HandleReport(t *testing.T) {
	server := setupMasterServer(t)
	jobID := "job-report-1"
	stageID := "stage-map"
	taskID := "task-success"
	taskFailID := "task-failure"

	// 1. Configuración de estado inicial: Job y Tareas en Running
	// Crear el Job en el Store
	server.Store.CreateJob(&common.JobRequest{JobID: jobID, NumPartitions: 1, DAG: common.DAG{Nodes: []common.OperationNode{{ID: stageID, NumPartitions: 1}}}})
	
	// Simular Tareas asignadas (para que el Scheduler las encuentre)
	runningTaskSuccess := common.Task{TaskID: taskID, JobID: jobID, StageID: stageID, Operation: common.OperationNode{ID: stageID}, RetryCount: 0}
	runningTaskFailure := common.Task{TaskID: taskFailID, JobID: jobID, StageID: stageID, Operation: common.OperationNode{ID: stageID}, RetryCount: 0}
	
	server.Scheduler.RunningTasks[taskID] = runningTaskSuccess
	server.Scheduler.RunningTasks[taskFailID] = runningTaskFailure

	// Reporte de Éxito
	t.Run("ReportSuccess_RemovesTaskAndCompletesJob", func(t *testing.T) {
		report := common.TaskReport{
			TaskID: taskID, JobID: jobID, StageID: stageID, Status: common.TaskStatusSuccess,
		}
		reportJSON, _ := json.Marshal(report)

		req := httptest.NewRequest("POST", "/report", bytes.NewBuffer(reportJSON))
		rr := httptest.NewRecorder()
		server.HandleReport(rr, req)

		// 1. Verificar que el Job finalizó (solo había 1 partición)
		status := server.Store.GetJob(jobID).Status
		if status != common.JobStatusSucceeded {
			t.Errorf("Reporte de éxito no finalizó el Job correctamente. Estado: %s", status)
		}
		// 2. Verificar que fue eliminado de RunningTasks
		if _, exists := server.Scheduler.RunningTasks[taskID]; exists {
			t.Errorf("La tarea exitosa no fue removida de RunningTasks.")
		}
	})
	
	// Reporte de Fallo
	t.Run("ReportFailure_TriggersRetry", func(t *testing.T) {
		report := common.TaskReport{
			TaskID: taskFailID, JobID: jobID, StageID: stageID, Status: common.TaskStatusFailure, ErrorMsg: "Fatal error",
		}
		reportJSON, _ := json.Marshal(report)

		req := httptest.NewRequest("POST", "/report", bytes.NewBuffer(reportJSON))
		rr := httptest.NewRecorder()
		server.HandleReport(rr, req)
		
		// 1. Verificar que la tarea fue removida de RunningTasks
		if _, exists := server.Scheduler.RunningTasks[taskFailID]; exists {
			t.Errorf("La tarea fallida no fue removida de RunningTasks.")
		}

		// 2. Verificar que la tarea fue re-encolada con retry count = 1
		if len(server.Scheduler.PendingTasks) != 1 || server.Scheduler.PendingTasks[0].TaskID != taskFailID {
			t.Errorf("El reporte de fallo no re-encoló la tarea.")
		}
		if server.Scheduler.PendingTasks[0].RetryCount != 1 {
			t.Errorf("El reintento no aumentó el contador de reintentos.")
		}
	})
}

*/