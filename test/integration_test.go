package integration_test

import (
	"bytes"
	"encoding/json"
	//"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"mini-spark/internal/common"
	"mini-spark/internal/worker"
)

// 1. Mock del Master
type MasterMock struct {
	ReportMutex    sync.Mutex
	ReceivedReport *common.TaskReport
	Server         *httptest.Server
}

func (m *MasterMock) handleReport(w http.ResponseWriter, r *http.Request) {
	var report common.TaskReport
	if err := json.NewDecoder(r.Body).Decode(&report); err != nil {
		http.Error(w, "JSON invalido", http.StatusBadRequest)
		return
	}

	m.ReportMutex.Lock()
	m.ReceivedReport = &report
	m.ReportMutex.Unlock()

	w.WriteHeader(http.StatusOK)
}

// Helper necesario (añadido porque faltaba en tu versión anterior)
func readOutputFile(t *testing.T, path string) string {
	content, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("No se pudo leer el archivo de salida: %v", err)
	}
	return string(content)
}

func createInputFile(t *testing.T, dir, filename, content string) string {
	path := filepath.Join(dir, filename)
	err := os.WriteFile(path, []byte(content), 0644)
	if err != nil {
		t.Fatalf("No se pudo crear el archivo de entrada: %v", err)
	}
	return path
}

// Helper para crear request de reporte (para el mock)
func createReportRequest(report common.TaskReport) *http.Request {
    reportJSON, _ := json.Marshal(report)
    return httptest.NewRequest("POST", "/report", bytes.NewBuffer(reportJSON))
}

func TestE2EFlow(t *testing.T) {
	// Setup Master Mock
	masterMock := &MasterMock{}
	// No necesitamos arrancar el servidor HTTP del Master Mock porque interceptaremos
	// la llamada directamente, pero es buena práctica tener la estructura.

	// Setup Entorno
	tempDir := t.TempDir()
	inputContent := "dato uno\ndato dos\n"
	expectedOutputContent := "DATO UNO\nDATO DOS\n" // Ojo con los saltos de línea

	inputPath := createInputFile(t, tempDir, "input.txt", inputContent)
	outputPath := filepath.Join(tempDir, "output_task.txt")

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
			Path: outputPath,
		},
	}

	// --- MOCKING (Interceptar la red) ---
	
	// Guardamos la función original para restaurarla al final
	originalReportFn := worker.ReportToMaster
	
	// Sobrescribimos la variable ReportToMaster.
	// Ahora, en lugar de ir a la red, llama a nuestro MasterMock directamente.
	worker.ReportToMaster = func(report common.TaskReport) error {
		// Simulamos la llamada HTTP recibida por el Master
		req := createReportRequest(report)
		w := httptest.NewRecorder()
		masterMock.handleReport(w, req)
		return nil
	}
	// Restaurar siempre al terminar el test
	defer func() { worker.ReportToMaster = originalReportFn }()

	// --- EJECUCIÓN ---

	// Simulamos que llega la petición HTTP al Worker
	taskJSON, _ := json.Marshal(task)
	req := httptest.NewRequest("POST", "/tasks", bytes.NewBuffer(taskJSON))
	rr := httptest.NewRecorder()

	// Llamamos al handler del Worker directamente
	worker.HandleTaskAssignment(rr, req)

	// --- VERIFICACIÓN ---

	// 1. El Worker respondió 200 OK?
	if rr.Code != http.StatusOK {
		t.Fatalf("Worker devolvió error HTTP: %d Body: %s", rr.Code, rr.Body.String())
	}

	// 2. El Master recibió el reporte?
	masterMock.ReportMutex.Lock()
	report := masterMock.ReceivedReport
	masterMock.ReportMutex.Unlock()

	if report == nil {
		t.Fatal("El Master Mock no recibió ningún reporte.")
	}

	if report.Status != common.TaskStatusSuccess {
		t.Errorf("Estado incorrecto. Esperado SUCCESS, Recibido %s. Error: %s", report.Status, report.ErrorMessage)
	}
	
	if report.OutputPath != outputPath {
		t.Errorf("Path incorrecto en reporte. Esperado %s, Recibido %s", outputPath, report.OutputPath)
	}

	// 3. El archivo se creó correctamente?
	actualOutput := readOutputFile(t, outputPath)
	
	// Normalización básica de saltos de linea
	expected := strings.TrimSpace(expectedOutputContent)
	actual := strings.TrimSpace(actualOutput)

	if actual != expected {
		t.Errorf("Contenido incorrecto.\nEsperado:\n%q\nRecibido:\n%q", expected, actual)
	}
}