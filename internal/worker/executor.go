package worker

import (
	"bufio"
	"encoding/json"
	"fmt"
	"hash/fnv"
	//"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	//"sync"
	"time"

	"mini-spark/internal/common"
	"mini-spark/internal/udf"
)

// ==========================================
// 1. GESTIÓN DEL POOL DE HILOS (WORKER POOL)
// ==========================================

// ExecutionManager controla la concurrencia en el nodo.
type ExecutionManager struct {
	maxThreads int
	semaphore  chan struct{} // Semáforo para limitar concurrencia
}

var GlobalExecutor *ExecutionManager

// InitExecutor inicializa el pool global con un límite de hilos.
func InitExecutor(maxThreads int) {
	GlobalExecutor = &ExecutionManager{
		maxThreads: maxThreads,
		semaphore:  make(chan struct{}, maxThreads),
	}
	log.Printf("[Executor] Inicializado pool con %d hilos", maxThreads)
}

// Submit intenta ejecutar una tarea. Bloquea si el pool está lleno.
func (e *ExecutionManager) Submit(task common.Task) ([]common.ShuffleMeta, error) {
	e.semaphore <- struct{}{} // Adquirir token (bloquea si está lleno)
	defer func() { <-e.semaphore }() // Liberar token al salir

	log.Printf("[Executor] Iniciando tarea %s (Threads activos: %d/%d)", task.TaskID, len(e.semaphore), e.maxThreads)
	return executeTaskLogic(task)
}

// ==========================================
// 2. LÓGICA PRINCIPAL DE EJECUCIÓN
// ==========================================

func executeTaskLogic(task common.Task) ([]common.ShuffleMeta, error) {
	switch task.Operation.Type {
	case common.OpTypeMap, common.OpTypeFilter, common.OpTypeFlatMap:
		return executeMapSide(task)
	case common.OpTypeReduceByKey, common.OpTypeJoin:
		return executeReduceSide(task)
	default:
		return nil, fmt.Errorf("operación no soportada: %s", task.Operation.Type)
	}
}

// ------------------------------------------
// LADO MAP (Map, Filter, FlatMap)
// ------------------------------------------
func executeMapSide(task common.Task) ([]common.ShuffleMeta, error) {
	inputFile, err := os.Open(task.InputPartition.Path)
	if err != nil {
		return nil, fmt.Errorf("error leyendo input: %w", err)
	}
	defer inputFile.Close()

	// Preparar escritores particionados (Buckets)
	writers, files, paths := createPartitionWriters(task)
	defer closeWriters(writers, files)

	// Obtener la UDF correspondiente
	var processFn func(udf.Record) []udf.Record
	
	switch task.Operation.Type {
	case common.OpTypeMap:
		fn, err := udf.GetMapFunction(task.Operation.UDFName)
		if err != nil { return nil, err }
		processFn = fn
	case common.OpTypeFlatMap:
		fn, err := udf.GetFlatMapFunction(task.Operation.UDFName)
		if err != nil { return nil, err }
		processFn = fn
	case common.OpTypeFilter:
		fn, err := udf.GetFilterFunction(task.Operation.UDFName)
		if err != nil { return nil, err }
		processFn = func(r udf.Record) []udf.Record {
			if fn(r) { return []udf.Record{r} }
			return nil
		}
	}

	//=================================
	scanner := bufio.NewScanner(inputFile)
	//spliting
	lineCounter := 0
	totalPartitions := task.Operation.NumPartitions
	if totalPartitions <= 0 { totalPartitions = 1 }
	// ---------------------------

	for scanner.Scan() {
		// FILTRO DE PARTICIÓN
		if lineCounter % totalPartitions == task.PartitionIndex {
			
			// PROCESAR SOLO SI ME TOCA
			line := scanner.Text()
			results := processFn(udf.Record(line))

			for _, res := range results {
				partID := 0
				if task.OutputTarget.Type == common.OutputTypeShuffle {
					partID = getPartitionID(string(res), task.OutputTarget.NumPartitions)
				}
				if w, ok := writers[partID]; ok {
					w.WriteString(string(res) + "\n")
				}
			}
		}
		lineCounter++
	}

	// Procesar línea por línea
	//scanner := bufio.NewScanner(inputFile)
	numPartitions := task.OutputTarget.NumPartitions
	if numPartitions <= 0 { numPartitions = 1 }

	for scanner.Scan() {
		results := processFn(udf.Record(scanner.Text()))
		for _, res := range results {
			// Particionamiento Hash
			partID := 0
			if task.OutputTarget.Type == common.OutputTypeShuffle {
				partID = getPartitionID(string(res), numPartitions)
			}
			// Escribir al bucket correspondiente
			if writer, ok := writers[partID]; ok {
				writer.WriteString(string(res) + "\n")
			} else {
				// Lazy creation si falló el setup inicial (safety check)
				// En esta implementación createPartitionWriters ya crea todo o nada, 
				// pero aquí podríamos manejar particiones dinámicas.
			}
		}
	}

	if err := scanner.Err(); err != nil { return nil, err }

	// Generar reporte
	return generateMeta(writers, files, paths)
}

// ------------------------------------------
// LADO REDUCE (ReduceByKey, Join)
// ------------------------------------------
func executeReduceSide(task common.Task) ([]common.ShuffleMeta, error) {
	// 1. Descarga (Shuffle Fetch) y Agregación en Memoria con Spill
	aggregator := NewMemoryAggregator(50 * 1024 * 1024) // 50MB Limite antes de Spill
	defer aggregator.Cleanup()

	// Descargar datos de todos los workers remotos
	for _, url := range task.InputPartition.ShuffleMap {
		if err := downloadAndMerge(url, aggregator); err != nil {
			log.Printf("[Warn] Fallo descargando parte del shuffle %s: %v", url, err)
			return nil, err // En un sistema real, aquí reintentaríamos
		}
	}

	// 2. Ejecutar Operación Final (Reduce o Join)
	outPath := fmt.Sprintf("%s_%s_out", task.OutputTarget.Path, task.TaskID)
	os.MkdirAll(filepath.Dir(outPath), 0755)
	outFile, err := os.Create(outPath)
	if err != nil { return nil, err }
	defer outFile.Close()
	writer := bufio.NewWriter(outFile)

	// Iterar sobre datos agregados
	dataMap := aggregator.GetDataMap() // Nota: Implementación simplificada sin merge-sort externo completo

	if task.Operation.Type == common.OpTypeReduceByKey {
		reduceFn, err := udf.GetReduceFunction(task.Operation.UDFName)
		if err != nil { return nil, err }

		for key, values := range dataMap {
			res := reduceFn(key, values)
			writer.WriteString(string(res) + "\n")
		}
	} else if task.Operation.Type == common.OpTypeJoin {
		joinFn, err := udf.GetJoinFunction(task.Operation.UDFName)
		if err != nil { return nil, err }
		
		// Para Join, necesitamos separar valores Left y Right.
		// Asumimos que el Map anterior etiquetó los datos o usamos una heurística.
		// SIMPLIFICACIÓN: Asumimos que los valores tienen prefijo "L:" o "R:"
		// O simplemente pasamos todo a la UDF y ella decide.
		for key, values := range dataMap {
			// En un sistema real, el KeyValue tendría metadatos de origen.
			// Aquí pasamos la lista cruda a una UDF genérica que dividiría manualmente.
			// Para cumplir el contrato, pasamos 'values' como left y 'nil' como right si no distinguimos,
			// o implementamos lógica de separación aquí.
			
			// Ejemplo dummy: Todos son left
			results := joinFn(key, values, []string{}) 
			for _, r := range results {
				writer.WriteString(string(r) + "\n")
			}
		}
	}

	writer.Flush()
	info, _ := outFile.Stat()
	return []common.ShuffleMeta{{PartitionKey: 0, Path: outPath, Size: info.Size()}}, nil
}

// ==========================================
// 3. GESTIÓN DE MEMORIA Y SPILL (Básico)
// ==========================================

type MemoryAggregator struct {
	data      map[string][]string
	sizeBytes int64
	limit     int64
	spillFiles []string
}

func NewMemoryAggregator(limit int64) *MemoryAggregator {
	return &MemoryAggregator{
		data:  make(map[string][]string),
		limit: limit,
	}
}

func (m *MemoryAggregator) Add(key, value string) {
	m.data[key] = append(m.data[key], value)
	m.sizeBytes += int64(len(key) + len(value))
	
	if m.sizeBytes > m.limit {
		m.SpillToDisk()
	}
}

func (m *MemoryAggregator) SpillToDisk() {
	// Implementación básica: Volcar mapa actual a un archivo temp y limpiar RAM
	tmpName := fmt.Sprintf("/tmp/spill_%d_%d.jsonl", time.Now().UnixNano(), len(m.spillFiles))
	f, _ := os.Create(tmpName)
	w := bufio.NewWriter(f)
	
	for k, vals := range m.data {
		for _, v := range vals {
			kv := common.KeyValue{Key: k, Value: v}
			b, _ := json.Marshal(kv)
			w.WriteString(string(b) + "\n")
		}
	}
	w.Flush()
	f.Close()
	
	m.spillFiles = append(m.spillFiles, tmpName)
	m.data = make(map[string][]string) // Reset memoria
	m.sizeBytes = 0
	log.Printf("[Executor] Spill to disk: %s", tmpName)
}

func (m *MemoryAggregator) GetDataMap() map[string][]string {
	// Si hubo spills, deberíamos recargarlos. 
	// SIMPLIFICACIÓN: Cargamos todo de vuelta a memoria (asumiendo que cabe para el Reduce final).
	// En prod: Usar External Merge Sort.
	for _, path := range m.spillFiles {
		f, _ := os.Open(path)
		sc := bufio.NewScanner(f)
		for sc.Scan() {
			var kv common.KeyValue
			json.Unmarshal(sc.Bytes(), &kv)
			m.data[kv.Key] = append(m.data[kv.Key], kv.Value)
		}
		f.Close()
	}
	return m.data
}

func (m *MemoryAggregator) Cleanup() {
	for _, p := range m.spillFiles { os.Remove(p) }
}

// ==========================================
// 4. HELPERS
// ==========================================

func downloadAndMerge(url string, agg *MemoryAggregator) error {
	resp, err := http.Get(url)
	if err != nil { return err }
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return fmt.Errorf("status %d from %s", resp.StatusCode, url)
	}

	sc := bufio.NewScanner(resp.Body)
	for sc.Scan() {
		var kv common.KeyValue
		if err := json.Unmarshal(sc.Bytes(), &kv); err == nil {
			agg.Add(kv.Key, kv.Value)
		}
	}
	return sc.Err()
}

func createPartitionWriters(task common.Task) (map[int]*bufio.Writer, map[int]*os.File, map[int]string) {
	writers := make(map[int]*bufio.Writer)
	files := make(map[int]*os.File)
	paths := make(map[int]string)

	numParts := task.OutputTarget.NumPartitions
	if numParts <= 0 { numParts = 1 }

	for i := 0; i < numParts; i++ {
		// Nombre: /ruta/base_TaskID_part_N
		p := fmt.Sprintf("%s_%s_part_%d", task.OutputTarget.Path, task.TaskID, i)
		os.MkdirAll(filepath.Dir(p), 0755)
		f, _ := os.Create(p)
		writers[i] = bufio.NewWriter(f)
		files[i] = f
		paths[i] = p
	}
	return writers, files, paths
}

func closeWriters(writers map[int]*bufio.Writer, files map[int]*os.File) {
	for _, w := range writers { w.Flush() }
	for _, f := range files { f.Close() }
}

func generateMeta(writers map[int]*bufio.Writer, files map[int]*os.File, paths map[int]string) ([]common.ShuffleMeta, error) {
	var metas []common.ShuffleMeta
	for pid, path := range paths {
		writers[pid].Flush()
		info, _ := files[pid].Stat()
		metas = append(metas, common.ShuffleMeta{
			PartitionKey: pid,
			Path:         path,
			Size:         info.Size(),
		})
	}
	return metas, nil
}

func getPartitionID(jsonLine string, numPartitions int) int {
	var kv common.KeyValue
	if err := json.Unmarshal([]byte(jsonLine), &kv); err == nil && kv.Key != "" {
		h := fnv.New32a()
		h.Write([]byte(kv.Key))
		return int(h.Sum32()) % numPartitions
	}
	return 0 // Fallback
}