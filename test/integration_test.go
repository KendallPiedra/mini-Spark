package integration_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"     // Usado por helpers
	"path/filepath"
	"strings"
	"sync"   // Usado por MasterMock
	"testing"

	"mini-spark/internal/common"
	"mini-spark/internal/worker"
)

// ==========================================================
//                 HELPER FUNCTIONS & MOCKS
// ==========================================================

// MasterMock simula el Master recibiendo el reporte del Worker
type MasterMock struct {
	ReceivedReport *common.TaskReport
	ReportMutex    sync.Mutex
}

// handleReport es el handler de red del Master Mock
func (m *MasterMock) handleReport(w http.ResponseWriter, r *http.Request) {
	m.ReportMutex.Lock()
	defer m.ReportMutex.Unlock()

	var report common.TaskReport
	if err := json.NewDecoder(r.Body).Decode(&report); err != nil {
		http.Error(w, "JSON de reporte invalido", http.StatusBadRequest)
		return
	}
	m.ReceivedReport = &report
	w.WriteHeader(http.StatusOK)
	fmt.Fprint(w, "Reporte recibido")
}

// createInputFile crea un archivo temporal para el input de la tarea.
func createInputFile(t *testing.T, dir, filename, content string) string {
	path := filepath.Join(dir, filename)
	// Normalizamos saltos de línea
	content = strings.ReplaceAll(content, "\n", "\n")
	err := os.WriteFile(path, []byte(content), 0644)
	if err != nil {
		t.Fatalf("No se pudo crear input: %v", err)
	}
	return path
}

// readOutputFile lee el contenido de un archivo dado su path.
func readOutputFile(t *testing.T, path string) string {
	content, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("No se pudo leer el archivo de salida: %v", err)
	}
	return string(content)
}

// Helper para crear request de reporte (para el mock)
func createReportRequest(report common.TaskReport) *http.Request {
	reportJSON, _ := json.Marshal(report)
	return httptest.NewRequest("POST", "/report", bytes.NewBuffer(reportJSON))
}

// ==========================================================
//                     TEST CASE
// ==========================================================

// TestE2EFlow verifica un flujo simple de asignación, ejecución y reporte de una tarea MAP.
func TestE2EFlow(t *testing.T) {
	// Setup Master Mock
	masterMock := &MasterMock{}

	// Setup Entorno de archivos temporales
	tempDir := t.TempDir()
	inputContent := "dato uno\ndato dos\n"
	expectedOutputContent := "DATO UNO\nDATO DOS\n" 

	inputPath := createInputFile(t, tempDir, "input.txt", inputContent)
	
	// Ruta BASE que la tarea usa
	outputBasePath := filepath.Join(tempDir, "output_task.txt") 
    
    // Ruta REAL esperada que crea el Executor para una salida simple (_part_0)
    expectedReportPath := outputBasePath + "_part_0" 

	task := common.Task{
		TaskID: "task-123",
		JobID:  "job-abc",
		Operation: common.OperationNode{
			Type:    common.OpTypeMap,
			UDFName: "to_uppercase",
		},
		InputPartition: common.TaskInput{
			SourceType: common.SourceTypeFile,
			Path:       inputPath,
		},
		OutputTarget: common.TaskOutput{
			Type: common.OutputTypeLocalSpill, 
			Path: outputBasePath, 
            NumPartitions: 1, 
		},
	}

	// --- MOCKING (Interceptar la red de reporte) ---
	originalReportFn := worker.ReportToMaster
	worker.ReportToMaster = func(report common.TaskReport) error {
		req := createReportRequest(report)
		w := httptest.NewRecorder()
		masterMock.handleReport(w, req)
		return nil
	}
	defer func() { worker.ReportToMaster = originalReportFn }()

	// --- EJECUCIÓN (Llamar al handler del Worker directamente) ---
	taskJSON, _ := json.Marshal(task)
	req := httptest.NewRequest("POST", "/tasks", bytes.NewBuffer(taskJSON))
	rr := httptest.NewRecorder()
	
	worker.HandleTaskAssignment(rr, req) 

	// --- VERIFICACIÓN ---

	if rr.Code != http.StatusOK {
		t.Fatalf("Worker devolvió error HTTP: %d Body: %s", rr.Code, rr.Body.String())
	}

	masterMock.ReportMutex.Lock()
	report := masterMock.ReceivedReport
	masterMock.ReportMutex.Unlock()

	if report == nil {
		t.Fatal("El Master Mock no recibió ningún reporte.")
	}

    // 1. Verificar Estado
	if report.Status != common.TaskStatusSuccess {
		t.Fatalf("Estado incorrecto. Esperado %s, Recibido %s. Error: %s", common.TaskStatusSuccess, report.Status, report.ErrorMsg)
	}
	
    // 2. Verificar Ruta Reportada
	if report.OutputPath != expectedReportPath {
		t.Errorf("Path incorrecto en reporte.\nEsperado: %s\nRecibido: %s", expectedReportPath, report.OutputPath)
	}

	// 3. Verificar Contenido
	actualOutput := readOutputFile(t, report.OutputPath)
	
	expected := strings.TrimSpace(expectedOutputContent)
	actual := strings.TrimSpace(actualOutput)

	if actual != expected {
		t.Errorf("Contenido incorrecto.\nEsperado:\n%q\nRecibido:\n%q", expected, actual)
	}
}