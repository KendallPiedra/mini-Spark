package worker
/*
import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
	"fmt"
	"mini-spark/internal/common"
)

// ==========================================================
// MOCKS Y UTILS DE SETUP
// ==========================================================

// Mock para simular la respuesta del Master
var mockReportReceived bool
var mockReportStatus string

func mockMasterReport(w http.ResponseWriter, r *http.Request) {
	var report common.TaskReport
	json.NewDecoder(r.Body).Decode(&report)
	
	mockReportReceived = true
	mockReportStatus = report.Status
	
	w.WriteHeader(http.StatusOK)
}

// Global variable para el test de Heartbeat
var mockHeartbeatReceived = false

func mockMasterHeartbeat(w http.ResponseWriter, r *http.Request) {
	var hb common.Heartbeat
	json.NewDecoder(r.Body).Decode(&hb)
	mockHeartbeatReceived = true
	w.WriteHeader(http.StatusOK)
}

func setupMasterMockServer() *httptest.Server {
	mux := http.NewServeMux()
	mux.HandleFunc("/report", mockMasterReport)
	mux.HandleFunc("/heartbeat", mockMasterHeartbeat)
	return httptest.NewServer(mux)
}

// Función para simular ExecuteTask (Sustituye la lógica real)
func mockExecuteTask(shouldFail bool) ([]common.ShuffleMeta, error) {
	if shouldFail {
		return nil, fmt.Errorf("error simulado de ejecución")
	}
	// Devuelve metadatos simulados de éxito
	return []common.ShuffleMeta{
		{PartitionKey: 0, Path: "/tmp/output/task-0_part_0", Size: 1024},
	}, nil
}

// ==========================================================
// TESTS
// ==========================================================

func TestAPI_HandleTaskAssignment(t *testing.T) {
	// 1. Configurar Mock Master y Worker
	server := setupMasterMockServer()
	defer server.Close()
	
	// Sobrescribir variables globales para que el worker apunte al mock
	MasterURL = server.URL
	MyID = "worker-test-01"

	// 2. Sobrescribir el motor ExecuteTask para controlar el resultado
	originalExecuteLogic := executeTaskLogic // Guardar original
	defer func() { executeTaskLogic = originalExecuteLogic }() // Restaurar
	
	// Task mock mínima
	task := common.Task{
		TaskID:  "task-123",
		JobID:   "job-abc",
		StageID: "map",
		OutputTarget: common.TaskOutput{Type: common.OutputTypeShuffle},
	}
	taskJSON, _ := json.Marshal(task)

	// --- Caso 1: Éxito ---
	t.Run("Success_Reports", func(t *testing.T) {
		executeTaskLogic = func(t common.Task) ([]common.ShuffleMeta, error) {
			return mockExecuteTask(false)
		}
		mockReportReceived = false
		
		req := httptest.NewRequest("POST", "/tasks", bytes.NewBuffer(taskJSON))
		rr := httptest.NewRecorder()
		
		HandleTaskAssignment(rr, req)
		
		// Esperar a que la goroutine de ejecución termine y reporte
		time.Sleep(10 * time.Millisecond)

		if rr.Code != http.StatusOK {
			t.Fatalf("Handler devolvió %d, esperaba 200", rr.Code)
		}
		if !mockReportReceived || mockReportStatus != common.TaskStatusSuccess {
			t.Errorf("Reporte de éxito falló. Recibido: %t, Status: %s", mockReportReceived, mockReportStatus)
		}
		if atomic.LoadInt32(&activeTasks) != 0 {
			t.Errorf("El contador de tareas activas no se limpió (Tasks: %d)", atomic.LoadInt32(&activeTasks))
		}
	})

	// --- Caso 2: Falla ---
	t.Run("Failure_Reports", func(t *testing.T) {
		executeTaskLogic = func(t common.Task) ([]common.ShuffleMeta, error) {
			return mockExecuteTask(true)
		}
		mockReportReceived = false
		
		req := httptest.NewRequest("POST", "/tasks", bytes.NewBuffer(taskJSON))
		rr := httptest.NewRecorder()
		
		HandleTaskAssignment(rr, req)
		time.Sleep(10 * time.Millisecond)

		if mockReportStatus != common.TaskStatusFailure {
			t.Errorf("Reporte de falla falló. Esperaba %s, obtuvo %s", common.TaskStatusFailure, mockReportStatus)
		}
	})
}

func TestAPI_Heartbeat(t *testing.T) {
	// Configurar Mock Master
	server := setupMasterMockServer()
	defer server.Close()
	MasterURL = server.URL
	MyID = "worker-test-hb"

	// Lanzar un solo ciclo de heartbeat (No el loop infinito)
	sendHeartbeat()
	
	// Esperar que el post asíncrono termine
	time.Sleep(5 * time.Millisecond) 

	if !mockHeartbeatReceived {
		t.Errorf("El Heartbeat no fue recibido por el Master Mock.")
	}
}

func TestAPI_HandleShuffleFetch(t *testing.T) {
	// Crear un archivo temporal para servir
	tempFile := filepath.Join(t.TempDir(), "shuffle_data.txt")
	os.WriteFile(tempFile, []byte("contenido secreto"), 0644)

	// Solicitud al endpoint /shuffle
	req := httptest.NewRequest("GET", "/shuffle?path="+tempFile, nil)
	rr := httptest.NewRecorder()
	
	handleShuffleFetch(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("HandleShuffleFetch falló. Código: %d", rr.Code)
	}
	if rr.Body.String() != "contenido secreto" {
		t.Errorf("Contenido incorrecto. Obtenido: %s", rr.Body.String())
	}
}
	*/