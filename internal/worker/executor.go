package worker

import (
	"bufio"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
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

type ExecutionManager struct {
	maxThreads int
	semaphore  chan struct{}
}

var GlobalExecutor *ExecutionManager

func InitExecutor(maxThreads int) {
	GlobalExecutor = &ExecutionManager{
		maxThreads: maxThreads,
		semaphore:  make(chan struct{}, maxThreads),
	}
	log.Printf("[Executor] Inicializado pool con %d hilos", maxThreads)
}

func (e *ExecutionManager) Submit(task common.Task) ([]common.ShuffleMeta, error) {
	e.semaphore <- struct{}{}
	defer func() { <-e.semaphore }()

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
	var inputStream io.ReadCloser
	var err error

	// 1. Determinar Fuente de Entrada (Archivo Local o Shuffle Remoto)
	if task.InputPartition.SourceType == common.SourceTypeShuffle {
		// Caso Especial: Etapa intermedia (ej. Map después de Filter)
		// Descargamos todo el shuffle a un archivo temporal para procesarlo linealmente
		tempPath := fmt.Sprintf("/tmp/shuffle_in_%s.tmp", task.TaskID)
		if err := downloadShuffleToTemp(task.InputPartition.ShuffleMap, tempPath); err != nil {
			return nil, fmt.Errorf("error descargando input shuffle: %w", err)
		}
		// Abrimos el temp
		inputStream, err = os.Open(tempPath)
		// Borramos el temp al terminar
		defer os.Remove(tempPath)
	} else {
		// Caso Normal: Lectura de archivo local (Source)
		inputStream, err = os.Open(task.InputPartition.Path)
	}

	if err != nil {
		return nil, fmt.Errorf("error leyendo input: %w", err)
	}
	defer inputStream.Close()

	// 2. Preparar Writers (Salida)
	writers, files, paths := createPartitionWriters(task)
	defer closeWriters(writers, files)

	// 3. Obtener UDF
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

	// 4. Procesar
	scanner := bufio.NewScanner(inputStream)
	
	// Configuración de Input Splitting (Solo aplica si leemos de ARCHIVO compartido)
	// Si viene de Shuffle, procesamos TODO lo que descargamos (ya viene particionado para mí)
	shouldSplit := (task.InputPartition.SourceType == common.SourceTypeFile)
	
	lineCounter := 0
	totalPartitions := task.Operation.NumPartitions
	if totalPartitions <= 0 { totalPartitions = 1 }

	for scanner.Scan() {
		// Input Splitting: Si es archivo compartido, solo leo mi parte.
		// Si es Shuffle, leo todo lo que me mandaron.
		if !shouldSplit || (lineCounter % totalPartitions == task.PartitionIndex) {
			
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

	if err := scanner.Err(); err != nil { return nil, err }

	return generateMeta(writers, files, paths)
}

// ------------------------------------------
// LADO REDUCE (ReduceByKey, Join)
// ------------------------------------------
func executeReduceSide(task common.Task) ([]common.ShuffleMeta, error) {
	// Agregación en Memoria con Spill
	aggregator := NewMemoryAggregator(50 * 1024 * 1024) 
	defer aggregator.Cleanup()

	// Descargar datos
	for _, url := range task.InputPartition.ShuffleMap {
		if err := downloadAndMerge(url, aggregator); err != nil {
			log.Printf("[Warn] Fallo parcial shuffle %s: %v", url, err)
			return nil, err
		}
	}

	// Salida
	outPath := fmt.Sprintf("%s_%s_out", task.OutputTarget.Path, task.TaskID)
	os.MkdirAll(filepath.Dir(outPath), 0755)
	outFile, err := os.Create(outPath)
	if err != nil { return nil, err }
	defer outFile.Close()
	writer := bufio.NewWriter(outFile)

	dataMap := aggregator.GetDataMap()

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
		
		for key, values := range dataMap {
			// En un sistema real separaríamos Left/Right aqui.
			// Pasamos todo y la UDF se encarga.
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
// 3. GESTIÓN DE MEMORIA Y HELPERS
// ==========================================

// Helper nuevo para descargar múltiples fuentes a un solo archivo (Para Map-Shuffle)
func downloadShuffleToTemp(shuffleMap map[string]string, destPath string) error {
	f, err := os.Create(destPath)
	if err != nil { return err }
	defer f.Close()
	
	w := bufio.NewWriter(f)

	for _, url := range shuffleMap {
		resp, err := http.Get(url)
		if err != nil { return err } // Fail fast
		
		if resp.StatusCode != 200 {
			resp.Body.Close()
			return fmt.Errorf("status %d from %s", resp.StatusCode, url)
		}

		// Copiar contenido al archivo temp
		_, err = io.Copy(w, resp.Body)
		resp.Body.Close()
		if err != nil { return err }
		
		// Asegurar salto de línea entre archivos
		w.WriteString("\n") 
	}
	w.Flush()
	return nil
}

// Agregador en Memoria con Spill a Disco
type MemoryAggregator struct {
	data      map[string][]string
	sizeBytes int64
	limit     int64
	spillFiles []string
}
// Crear un nuevo agregador en memoria con límite de tamaño
func NewMemoryAggregator(limit int64) *MemoryAggregator {
	return &MemoryAggregator{
		data:  make(map[string][]string),
		limit: limit,
	}
}

// Agrega un par clave-valor al agregador en memoria
func (m *MemoryAggregator) Add(key, value string) {
	m.data[key] = append(m.data[key], value)
	m.sizeBytes += int64(len(key) + len(value))
	if m.sizeBytes > m.limit { 
		m.SpillToDisk() 
	}
}

func (m *MemoryAggregator) SpillToDisk() {
	// Generar un nombre de archivo temporal único
	tmpName := fmt.Sprintf("/tmp/spill_%d_%d.jsonl", time.Now().UnixNano(), len(m.spillFiles))
	f, err := os.Create(tmpName)
	if err != nil {
		log.Printf("[ERROR] Fallo al crear archivo de spill: %v", err)
		return
	}
	w := bufio.NewWriter(f)
	
	// Serializar cada par K/V como una línea JSON (JSON Lines - JSONL)
	for k, vals := range m.data {
		for _, v := range vals {
			kv := common.KeyValue{Key: k, Value: v}
			b, _ := json.Marshal(kv)
			w.WriteString(string(b) + "\n")
		}
	}
	
	w.Flush()
	f.Close()
	
	// Actualizar estado del agregador
	m.spillFiles = append(m.spillFiles, tmpName)
	m.data = make(map[string][]string) // Vaciar la memoria
	m.sizeBytes = 0                    // Resetear el contador
	log.Printf("[Executor] Spill a disco: %s", tmpName)
}

// GetDataMap recupera los datos espilleados del disco y los fusiona con los datos en memoria.
func (m *MemoryAggregator) GetDataMap() map[string][]string {
	// Recuperar datos de todos los archivos de spill
	for _, path := range m.spillFiles {
		f, err := os.Open(path)
		if err != nil {
			log.Printf("[ERROR] No se pudo abrir archivo de spill %s: %v", path, err)
			continue
		}
		sc := bufio.NewScanner(f)
		
		// Deserializar JSON Lines y fusionar en el mapa 'data'
		for sc.Scan() {
			var kv common.KeyValue
			if err := json.Unmarshal(sc.Bytes(), &kv); err == nil {
				m.data[kv.Key] = append(m.data[kv.Key], kv.Value)
			}
		}
		f.Close()
	}
	// El mapa 'data' ahora contiene todos los datos agregados (Memoria + Disco)
	return m.data
}

func (m *MemoryAggregator) Cleanup() {
	for _, p := range m.spillFiles { 
		os.Remove(p) 
	}
}

func downloadAndMerge(url string, agg *MemoryAggregator) error {
	resp, err := http.Get(url)
	if err != nil { return err }
	defer resp.Body.Close()
	if resp.StatusCode != 200 { return fmt.Errorf("status %d", resp.StatusCode) }

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
	return 0
}