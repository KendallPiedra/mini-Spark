package worker

import (
	"bufio"
	"fmt"
	"os"
	"strings"

	"mini-spark/internal/common" 
	"mini-spark/internal/udf" // Importamos el registro de funciones
)

// ExecuteTask recibe una tarea y la ejecuta en el Worker.
// Devuelve la ruta del archivo de salida intermedio o un error.
func ExecuteTask(task common.Task) (string, error) {
	// 1. Validar y preparar rutas
	inputPath := task.InputPartition.Path
	outputPath := task.OutputTarget.Path

	// 2. Crear el directorio de salida si no existe
	outputDir := outputPath[:strings.LastIndex(outputPath, "/")]
	if err := os.MkdirAll(outputDir, 0755); err != nil {
		return "", fmt.Errorf("error al crear el directorio de salida: %w", err)
	}

	// 3. Abrir archivos de entrada y salida
	inputFile, err := os.Open(inputPath)
	if err != nil {
		return "", fmt.Errorf("error al abrir el archivo de entrada %s: %w", inputPath, err)
	}
	defer inputFile.Close()

	outputFile, err := os.Create(outputPath)
	if err != nil {
		return "", fmt.Errorf("error al crear el archivo de salida: %w", err)
	}
	defer outputFile.Close()
	writer := bufio.NewWriter(outputFile)
	defer writer.Flush()

	// 4. Obtener la UDF a ejecutar
	var processor func(udf.Record) []udf.Record
	
	switch task.Operation.Type {
	case common.OpTypeMap:
		mapFn, err := udf.GetMapFunction(task.Operation.UDFName)
		if err != nil {
			return "", err
		}
		processor = mapFn
	case common.OpTypeFilter:
		filterFn, err := udf.GetFilterFunction(task.Operation.UDFName)
		if err != nil {
			return "", err
		}
		// Para FILTER, adaptamos la función a la firma de MAP:
		processor = func(r udf.Record) []udf.Record {
			if filterFn(r) {
				return []udf.Record{r} // Si pasa el filtro, mantenemos el registro
			}
			return nil // Si no pasa, descartamos
		}
	default:
		return "", fmt.Errorf("tipo de operacion no soportada por el executor: %s", task.Operation.Type)
	}

	// 5. Lectura, procesamiento y escritura línea por línea
	scanner := bufio.NewScanner(inputFile)
	
	// NOTA: Ignoramos los Offsets por ahora, simulando que leemos el archivo completo.
	// En una fase posterior, ajustaremos el scanner para leer solo el rango de bytes.

	for scanner.Scan() {
		line := scanner.Text()
		
		// 6. Ejecutar la UDF
		results := processor(udf.Record(line))

		// 7. Escribir los resultados de la UDF
		for _, result := range results {
			if _, err := writer.WriteString(string(result) + "\n"); err != nil {
				return "", fmt.Errorf("error al escribir en el archivo de salida: %w", err)
			}
		}
	}

	if err := scanner.Err(); err != nil {
		return "", fmt.Errorf("error al leer el archivo de entrada: %w", err)
	}

	return outputPath, nil
}