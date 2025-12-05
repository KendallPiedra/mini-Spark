package worker

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"mini-spark/internal/common" 
	//"mini-spark/internal/udf" // Necesario para que el ExecuteTask pueda acceder a las UDFs
)

// Helper para crear un archivo de entrada con contenido en un directorio temporal
func createInputFile(t *testing.T, dir, filename, content string) string {
	path := filepath.Join(dir, filename)
	// Reemplazar \n con el formato de línea del sistema operativo
	content = strings.ReplaceAll(content, "\n", "\n") 
	err := os.WriteFile(path, []byte(content), 0644)
	if err != nil {
		t.Fatalf("No se pudo crear el archivo de entrada: %v", err)
	}
	return path
}

// Helper para leer el contenido del archivo de salida
func readOutputFile(t *testing.T, path string) string {
	content, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("No se pudo leer el archivo de salida: %v", err)
	}
	return string(content)
}

// Helper para crear una Task mock
func createMockTask(jobID, inputPath, outputPath, opType, udfName string) common.Task {
	return common.Task{
		TaskID:  "task-" + jobID,
		JobID:   jobID,
		Operation: common.OperationNode{
			Type: opType,
			UDFName: udfName,
		},
		InputPartition: common.TaskInput{
			SourceType: common.SourceTypeFile,
			Path: inputPath,
		},
		OutputTarget: common.TaskOutput{
			Type: common.OutputTypeLocalSpill,
			Path: outputPath,
		},
	}
}

func TestExecuteTask(t *testing.T) {
	// Usamos \n como separador estándar en el test para evitar problemas de OS
	inputContent := "Linea uno\nlinea dos\n\nLinea tres"
	
	tests := []struct {
		name          string
		opType        string
		udfName       string
		inputContent  string
		expectedOutput string
		expectErr     bool
	}{
		{
			name: "MAP_to_uppercase_Exitoso",
			opType: common.OpTypeMap,
			udfName: "to_uppercase",
			inputContent: inputContent,
			// La UDF toma el registro, lo mapea y el Executor añade un \n
			expectedOutput: "LINEA UNO\nLINEA DOS\n\nLINEA TRES\n", 
			expectErr: false,
		},
		{
			name: "FILTER_not_empty_Exitoso",
			opType: common.OpTypeFilter,
			udfName: "not_empty",
			inputContent: inputContent,
			// Las líneas vacías son eliminadas por el filtro
			expectedOutput: "Linea uno\nlinea dos\nLinea tres\n", 
			expectErr: false,
		},
		{
			name: "UDF_No_Encontrada_Error",
			opType: common.OpTypeMap,
			udfName: "non_existent_func",
			inputContent: "test",
			expectedOutput: "",
			expectErr: true,
		},
		{
			name: "Tipo_Op_No_Soportado_Error",
			opType: common.OpTypeReduceByKey, // No soportado por el Executor en esta fase
			udfName: "to_uppercase",
			inputContent: "test",
			expectedOutput: "",
			expectErr: true,
		},
		{
			name: "Archivo_de_Entrada_No_Existe_Error",
			opType: common.OpTypeMap,
			udfName: "to_uppercase",
			inputContent: "Simulado", // El contenido es irrelevante, probamos el error de I/O
			expectedOutput: "",
			expectErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tempDir := t.TempDir()
			outputFilename := "output.txt"
			
			var inputPath string
			if tt.name != "Archivo_de_Entrada_No_Existe_Error" {
				inputPath = createInputFile(t, tempDir, "input.txt", tt.inputContent)
			} else {
				// Simular un path que no existe para forzar el error de os.Open
				inputPath = filepath.Join(tempDir, "non_existent_input.txt")
			}
			
			outputPath := filepath.Join(tempDir, outputFilename)

			task := createMockTask("job-1", inputPath, outputPath, tt.opType, tt.udfName)

			// Ejecutar la tarea
			actualPath, err := ExecuteTask(task)

			if (err != nil) != tt.expectErr {
				t.Fatalf("ExecuteTask falló. Se esperaba error: %t, Obtenido: %v", tt.expectErr, err)
			}

			if !tt.expectErr {
				if actualPath != outputPath {
					t.Errorf("Ruta de salida incorrecta. Esperado: %s, Obtenido: %s", outputPath, actualPath)
				}

				actualOutput := readOutputFile(t, actualPath)
				
				// Normalizamos los saltos de línea para la comparación final
				expected := strings.ReplaceAll(tt.expectedOutput, "\n", "\n")
				actual := strings.ReplaceAll(actualOutput, "\n", "\n")

				if actual != expected {
					t.Errorf("Contenido de salida incorrecto.\nEsperado:\n%q\nObtenido:\n%q", expected, actual)
				}
			}
		})
	}
}