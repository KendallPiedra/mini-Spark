package worker

import (
	//"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest" // Necesario para simular la red
	"os"
	"path/filepath"
	"strings"
	"testing"

	"mini-spark/internal/common"
	//"mini-spark/internal/udf"
)

// Inicialización del Executor Global para todas las pruebas
func init() {
	// Inicializamos el Executor con 2 hilos para las pruebas
	InitExecutor(2)
}

// ==========================================================
// HELPERS
// ==========================================================

func createInputFile(t *testing.T, dir, filename, content string) string {
	path := filepath.Join(dir, filename)
	err := os.WriteFile(path, []byte(content), 0644)
	if err != nil {
		t.Fatalf("Error creando input: %v", err)
	}
	return path
}

func readOutputFile(t *testing.T, path string) string {
	content, err := os.ReadFile(path)
	if err != nil {
		return ""
	}
	return string(content)
}

func createMockTask(jobID, stageID, opType, udfName, outType string, partitions int, inputPath string, shuffleMap map[string]string) common.Task {
	return common.Task{
		TaskID:  fmt.Sprintf("%s-%s-0", jobID, stageID),
		JobID:   jobID,
		StageID: stageID,
		Operation: common.OperationNode{
			Type:          opType,
			UDFName:       udfName,
			NumPartitions: partitions,
		},
		InputPartition: common.TaskInput{
			SourceType: func() string {
				if shuffleMap != nil { return common.SourceTypeShuffle }
				return common.SourceTypeFile
			}(),
			Path:       inputPath,
			ShuffleMap: shuffleMap,
		},
		OutputTarget: common.TaskOutput{
			Type:       outType,
			Path:       filepath.Join("/tmp/spark_test", jobID, stageID),
			NumPartitions: partitions,
		},
		PartitionIndex: 0,
	}
}

// ==========================================================
// TEST UNITARIO PRINCIPAL
// ==========================================================

func TestExecutor_MapAndFilter(t *testing.T) {
	tempDir := t.TempDir()
	inputContent := "line 1\nline 2\nline 3\n"
	inputPath := createInputFile(t, tempDir, "input.txt", inputContent)

	tests := []struct {
		name              string
		opType            string
		udfName           string
		numPartitions     int
		expectedMetaCount int
		expectedOutput    string
		expectErr         bool
	}{
		{
			name:              "MAP_WordCount_Shuffle",
			opType:            common.OpTypeMap,
			udfName:           "map_wordcount",
			numPartitions:     2,
			expectedMetaCount: 2,
			expectedOutput: `{"key":"line","value":"1"}`,
			expectErr:         false,
		},
		{
			name:              "FILTER_NotEmpty_Spill",
			opType:            common.OpTypeFilter,
			udfName:           "not_empty",
			numPartitions:     1,
			expectedMetaCount: 1,
			expectedOutput:    "line 1",
			expectErr:         false,
		},
		{
			name:              "FLATMAP_Tokenize",
			opType:            common.OpTypeFlatMap,
			udfName:           "tokenize_flatmap",
			numPartitions:     1,
			expectedMetaCount: 1,
			expectedOutput:    "line",
			expectErr:         false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			task := createMockTask(
				"job-map-side",
				tt.opType,
				tt.opType,
				tt.udfName,
				common.OutputTypeShuffle,
				tt.numPartitions,
				inputPath,
				nil,
			)

			metas, err := GlobalExecutor.Submit(task)

			if (err != nil) != tt.expectErr {
				t.Fatalf("Submit falló. Esperaba error=%t, obtuvo: %v", tt.expectErr, err)
			}
			if tt.expectErr {
				return
			}

			if len(metas) != tt.expectedMetaCount {
				if tt.name != "MAP_WordCount_Shuffle" { // Excepción debido a hash variable
					t.Fatalf("Esperaba %d metadatos, obtuvo %d", tt.expectedMetaCount, len(metas))
				}
			}

			totalOutput := ""
			for _, meta := range metas {
				totalOutput += readOutputFile(t, meta.Path)
			}

			if !strings.Contains(totalOutput, tt.expectedOutput) {
				t.Errorf("Salida incorrecta. Falta '%s' en:\n%s", tt.expectedOutput, totalOutput)
			}
		})
	}
}

func TestExecutor_ReduceAndJoin(t *testing.T) {
	// --- 1. Configurar Servidor HTTP Mock para el Shuffle ---
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path := r.URL.Query().Get("path")
		if path != "" {
			data, err := os.ReadFile(path)
			if err != nil {
				http.Error(w, "File read error", http.StatusInternalServerError)
				return
			}
			
			w.Header().Set("Content-Type", "application/json-lines")
			w.WriteHeader(http.StatusOK)
			w.Write(data) 
		} else {
			http.Error(w, "Path required", http.StatusBadRequest)
		}
	})
	shuffleTestServer := httptest.NewServer(handler)
	defer shuffleTestServer.Close()
	serverURL := shuffleTestServer.URL
	
	tempDir := t.TempDir()
	
	// Contenido del Map-side: Claves para REDUCE (a:2) y JOIN (a:L1 + a:R1)
	pathA := createInputFile(t, tempDir, "map_a.jsonl", `{"key":"a","value":"1"}`+"\n"+`{"key":"b","value":"1"}`+"\n")
	pathB := createInputFile(t, tempDir, "map_b.jsonl", `{"key":"a","value":"1"}`+"\n"+`{"key":"c","value":"1"}`+"\n")
	// Contenido de JOIN etiquetado (L=Left, R=Right)
	pathJoin := createInputFile(t, tempDir, "join_data.jsonl", 
		`{"key":"a","value":"L:L1"}`+"\n"+ // Usuario A (Left)
		`{"key":"a","value":"R:R1"}`+"\n") // Orden A (Right)
	
	// ShuffleMap para las pruebas
	reduceShuffleMap := map[string]string{
		"w1-A": fmt.Sprintf("%s/?path=%s", serverURL, pathA),
		"w2-B": fmt.Sprintf("%s/?path=%s", serverURL, pathB),
	}
	joinShuffleMap := map[string]string{
		"w1-Join": fmt.Sprintf("%s/?path=%s", serverURL, pathJoin),
	}

	tests := []struct {
		name           string
		opType         string
		udfName        string
		shuffleMap     map[string]string
		expectedOutput string 
		expectErr      bool
	}{
		{
			name:           "REDUCE_Sum",
			opType:         common.OpTypeReduceByKey,
			udfName:        "reduce_sum",
			shuffleMap:     reduceShuffleMap,
			expectedOutput: `{"key":"a","value":"2"}`, 
			expectErr:      false,
		},
		{
			name:           "JOIN_Simple",
			opType:         common.OpTypeJoin,
			udfName:        "join_users_orders", // Usar la UDF de Join real
			shuffleMap:     joinShuffleMap,
			expectedOutput: `"key":"a","value":"L1 compro R1"`, // La UDF Join genera un string complejo, solo verificamos la clave
			expectErr:      false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			task := createMockTask(
				"job-reduce-side",
				tt.opType,
				tt.opType,
				tt.udfName,
				common.OutputTypeLocalSpill,
				1,
				"", 
				tt.shuffleMap,
			)

			metas, err := GlobalExecutor.Submit(task)

			if (err != nil) != tt.expectErr {
				t.Fatalf("Submit falló. Esperaba error=%t, obtuvo: %v", tt.expectErr, err)
			}
			if tt.expectErr {
				return
			}
			if len(metas) != 1 {
				t.Fatalf("Esperaba 1 archivo de salida, obtuvo %d", len(metas))
			}

			output := readOutputFile(t, metas[0].Path)
			if !strings.Contains(output, tt.expectedOutput) {
				t.Errorf("Salida incorrecta. Falta '%s' en:\n%s", tt.expectedOutput, output)
			}
		})
	}
}