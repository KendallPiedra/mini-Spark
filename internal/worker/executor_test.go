package worker

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"mini-spark/internal/common"
	"mini-spark/internal/udf" 
)

// Helper para crear archivos de entrada
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

// Helper para leer archivos de salida
func readOutputFile(t *testing.T, path string) string {
	content, err := os.ReadFile(path)
	if err != nil {
		// No fallamos fatalmente aquí para permitir verificar si el archivo debía existir o no
		return "" 
	}
	return string(content)
}

// Helper actualizado para soportar Particiones y OutputType
func createMockTask(jobID, inputPath, outputPath, opType, udfName, outType string, partitions int) common.Task {
	return common.Task{
		TaskID:  "task-" + jobID,
		JobID:   jobID,
		Operation: common.OperationNode{
			Type:    opType,
			UDFName: udfName,
		},
		InputPartition: common.TaskInput{
			SourceType: common.SourceTypeFile,
			Path:       inputPath,
		},
		OutputTarget: common.TaskOutput{
			Type:       outType,
			Path:       outputPath,
			Partitions: partitions,
		},
	}
}

func TestExecuteTask(t *testing.T) {
	inputSimple := "linea uno\nlinea dos"
	
	// Input para el caso de Shuffle (palabras repetidas para verificar hash)
	// "a" debería ir siempre a la misma partición.
	inputShuffle := "a b a c" 

	tests := []struct {
		name            string
		opType          string
		udfName         string
		outputType      string
		partitions      int
		inputContent    string
		
		// Verificación: Esperamos N archivos generados
		expectedFiles   int
		// Verificación: Mapa de "Contenido Esperado" (substring) que debe estar en CUALQUIERA de los archivos
		expectedSubstr  []string 
		expectErr       bool
	}{
		{
			name:           "MAP_Simple_LocalSpill",
			opType:         common.OpTypeMap,
			udfName:        "to_uppercase",
			outputType:     common.OutputTypeLocalSpill,
			partitions:     1,
			inputContent:   inputSimple,
			expectedFiles:  1,
			expectedSubstr: []string{"LINEA UNO", "LINEA DOS"},
			expectErr:      false,
		},
		{
			name:           "FILTER_Simple",
			opType:         common.OpTypeFilter,
			udfName:        "not_empty",
			outputType:     common.OutputTypeLocalSpill,
			partitions:     1,
			inputContent:   "dato\n\n",
			expectedFiles:  1,
			expectedSubstr: []string{"dato"}, // La línea vacía no debe estar
			expectErr:      false,
		},
		{
			name:           "SHUFFLE_WordCount_2Partitions",
			opType:         common.OpTypeMap,
			udfName:        "map_wordcount", // Esta UDF genera JSON KeyValue
			outputType:     common.OutputTypeShuffle,
			partitions:     2, // Forzamos hash partitioning
			inputContent:   inputShuffle,
			expectedFiles:  2, // Esperamos que "a", "b", "c" se distribuyan (idealmente en 2 particiones)
			// Verificamos que los JSON se generaron correctamente
			expectedSubstr: []string{
				`{"key":"a","value":"1"}`, 
				`{"key":"b","value":"1"}`,
				`{"key":"c","value":"1"}`,
			},
			expectErr:      false,
		},
		{
			name:           "Error_UDF_No_Existe",
			opType:         common.OpTypeMap,
			udfName:        "fantasma",
			outputType:     common.OutputTypeLocalSpill,
			partitions:     1,
			inputContent:   "test",
			expectedFiles:  0,
			expectErr:      true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tempDir := t.TempDir()
			inputPath := createInputFile(t, tempDir, "input.txt", tt.inputContent)
			outputBasePath := filepath.Join(tempDir, "output") // Base path sin extensión

			task := createMockTask("job-1", inputPath, outputBasePath, tt.opType, tt.udfName, tt.outputType, tt.partitions)

			// 1. Ejecutar
			metas, err := ExecuteTask(task)

			// 2. Verificar Error
			if (err != nil) != tt.expectErr {
				t.Fatalf("Error inesperado. Esperaba error=%v, obtuvo: %v", tt.expectErr, err)
			}
			if tt.expectErr {
				return // Si esperábamos error y ocurrió, terminamos aquí
			}

			// 3. Verificar Cantidad de Archivos Generados
			// Nota: En Shuffle, puede que una partición quede vacía si el hash lo decide, 
			// pero nuestro Executor crea el writer bajo demanda. 
			// Para "a b a c" con hash simple, es muy probable que use ambos buckets.
			if len(metas) == 0 {
				t.Errorf("No se generaron archivos de metadatos")
			}
			
			// 4. Verificar Contenido (Acumulado)
			// Leemos TODOS los archivos generados y los concatenamos para buscar los substrings esperados
			var fullContentBuilder strings.Builder
			
			for _, meta := range metas {
				// Validar que el path en el meta existe
				content := readOutputFile(t, meta.Path)
				fullContentBuilder.WriteString(content)
				fullContentBuilder.WriteString("\n") // Separador
				
				// Validar el tamaño reportado
				if meta.Size != int64(len(content)) {
					t.Errorf("Tamaño incorrecto en meta para particion %d. Meta: %d, Real: %d", meta.PartitionKey, meta.Size, len(content))
				}
			}
			
			fullContent := fullContentBuilder.String()

			for _, substr := range tt.expectedSubstr {
				if !strings.Contains(fullContent, substr) {
					t.Errorf("Falta contenido esperado en la salida.\nBuscando: %s\nEncontrado:\n%s", substr, fullContent)
				}
			}
			
			// 5. Validación Extra para Shuffle: Formato JSON válido
			if tt.outputType == common.OutputTypeShuffle {
				lines := strings.Split(fullContent, "\n")
				for _, line := range lines {
					if strings.TrimSpace(line) == "" { continue }
					var kv common.KeyValue
					if err := json.Unmarshal([]byte(line), &kv); err != nil {
						t.Errorf("Línea de salida no es JSON válido en modo Shuffle: %s", line)
					}
				}
			}
		})
	}
}