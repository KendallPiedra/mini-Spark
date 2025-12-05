package worker

import (
	"bufio"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"os"
	"path/filepath"
	"strings"

	"mini-spark/internal/common"
	"mini-spark/internal/udf"
)

// ExecuteTask ejecuta la tarea y maneja tanto salida simple como Shuffle particionado.
// Retorna la lista de metadatos de los archivos generados.
func ExecuteTask(task common.Task) ([]common.ShuffleMeta, error) {
	// 1. Preparación de Input
	inputFile, err := os.Open(task.InputPartition.Path)
	if err != nil {
		return nil, fmt.Errorf("error input: %w", err)
	}
	defer inputFile.Close()

	// 2. Preparación de Output (Writers)
	// Mapa de PartitionID -> Writer
	writers := make(map[int]*bufio.Writer)
	files := make(map[int]*os.File)
	outputPaths := make(map[int]string)

	// Helper para cerrar archivos de forma segura al final
	defer func() {
		for _, w := range writers { w.Flush() }
		for _, f := range files { f.Close() }
	}()

	// Lógica para crear escritores según el tipo de salida
	numPartitions := 1
	if task.OutputTarget.Type == common.OutputTypeShuffle {
		numPartitions = task.OutputTarget.Partitions
		if numPartitions <= 0 { numPartitions = 1 }
	}

	// Función helper para obtener/crear el escritor de una partición
	getWriter := func(partID int) (*bufio.Writer, error) {
		if w, exists := writers[partID]; exists {
			return w, nil
		}
		
		// Construir nombre del archivo: path_base + "_part_X"
		// Ej: /tmp/job123_task4_part_0
		finalPath := fmt.Sprintf("%s_part_%d", task.OutputTarget.Path, partID)
		
		// Crear directorio padre si no existe
		if err := os.MkdirAll(filepath.Dir(finalPath), 0755); err != nil {
			return nil, err
		}

		f, err := os.Create(finalPath)
		if err != nil {
			return nil, err
		}
		
		w := bufio.NewWriter(f)
		
		files[partID] = f
		writers[partID] = w
		outputPaths[partID] = finalPath
		return w, nil
	}

	// 3. Obtener UDF
	var mapFn udf.UDFMapFn
	
	switch task.Operation.Type {
	case common.OpTypeMap:
		mapFn, err = udf.GetMapFunction(task.Operation.UDFName)
	case common.OpTypeFilter:
		// Adaptador de Filter a Map (ya visto anteriormente)
		filterFn, err2 := udf.GetFilterFunction(task.Operation.UDFName)
		err = err2
		mapFn = func(r udf.Record) []udf.Record {
			if filterFn(r) { return []udf.Record{r} }
			return nil
		}
	default:
		return nil, fmt.Errorf("operacion no soportada: %s", task.Operation.Type)
	}
	if err != nil { return nil, err }

	// 4. Procesamiento
	scanner := bufio.NewScanner(inputFile)
	for scanner.Scan() {
		line := scanner.Text()
		results := mapFn(udf.Record(line))

		for _, record := range results {
			targetPartition := 0
			
			// Lógica de Hashing para Shuffle
			if task.OutputTarget.Type == common.OutputTypeShuffle {
				// Intentar decodificar como KeyValue para hacer hash de la clave
				var kv common.KeyValue
				// Asumimos que el record es un JSON string
				if jsonErr := json.Unmarshal([]byte(record), &kv); jsonErr == nil && kv.Key != "" {
					targetPartition = int(hash(kv.Key)) % numPartitions
				} else {
					// Si no es KV, o falla, va a partición 0 (fallback)
					targetPartition = 0
				}
			}

			w, err := getWriter(targetPartition)
			if err != nil { return nil, err }

			// Escribir línea + salto de línea
			if _, err := w.WriteString(string(record) + "\n"); err != nil {
				return nil, err
			}
		}
	}

	if err := scanner.Err(); err != nil { return nil, err }

	// 5. Generar Reporte de Metadatos
	var metaList []common.ShuffleMeta
	for pid, path := range outputPaths {
		// Aseguramos flush antes de medir tamaño
		writers[pid].Flush()
		info, _ := files[pid].Stat()
		
		metaList = append(metaList, common.ShuffleMeta{
			PartitionKey: pid,
			Path:         path,
			Size:         info.Size(),
		})
	}

	return metaList, nil
}

// Función de Hash FNV-1a (Estándar simple y rápido)
func hash(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}