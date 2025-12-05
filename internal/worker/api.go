package worker

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"runtime" // NECESARIO PARA MÉTRICAS REALES
	"sync/atomic"
	"time"

	"mini-spark/internal/common"
)

// =========================================================
// VARIABLES GLOBALES Y CONFIGURACIÓN
// =========================================================

var (
	MasterURL   string
	MyID        string
	activeTasks int32
)

// Constantes de configuración
const (
	HeartbeatInterval = 3 * time.Second
	ShufflePathPrefix = "/shuffle"
	// REQUERIMIENTO PDF: Límite de tiempo por tarea.
	// En prod esto vendría en el JobRequest, aquí usamos un default seguro.
	TaskTimeout       = 10 * time.Second 
)

// =========================================================
// INICIO DEL SERVIDOR
// =========================================================

func StartServer(port int, masterAddress string) {
	MasterURL = masterAddress
	MyID = fmt.Sprintf("localhost:%d", port)

	// Inicializar el Ejecutor con 4 hilos (Requerimiento: Pool Configurable)
	InitExecutor(4)

	mux := http.NewServeMux()
	mux.HandleFunc("POST /tasks", HandleTaskAssignment)
	mux.HandleFunc("GET "+ShufflePathPrefix, handleShuffleFetch)
	
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		fmt.Fprintf(w, "Worker %s ONLINE. Tasks: %d", MyID, atomic.LoadInt32(&activeTasks))
	})

	go startHeartbeatLoop()

	log.Printf("[Worker %s] Listo en :%d (Pool: 4 threads, Timeout: %s)", MyID, port, TaskTimeout)
	if err := http.ListenAndServe(fmt.Sprintf(":%d", port), mux); err != nil {
		log.Fatal(err)
	}
}

// =========================================================
// HANDLERS HTTP
// =========================================================

func HandleTaskAssignment(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", 405); return
	}

	var task common.Task
	if err := json.NewDecoder(r.Body).Decode(&task); err != nil {
		http.Error(w, "JSON inválido", 400); return
	}

	atomic.AddInt32(&activeTasks, 1)
	log.Printf("[Worker] Recibida tarea %s (Op: %s)", task.TaskID, task.Operation.Type)

	// Ejecución asíncrona para no bloquear al Master
	go runTaskAsync(task)

	w.WriteHeader(http.StatusOK)
}

func handleShuffleFetch(w http.ResponseWriter, r *http.Request) {
	path := r.URL.Query().Get("path")
	if path == "" {
		http.Error(w, "Missing path", 400); return
	}
	// TODO: Validar que el path sea seguro (dentro de /tmp/mini-spark)
	log.Printf("[Shuffle] Sirviendo %s a %s", path, r.RemoteAddr)
	http.ServeFile(w, r, path)
}

// =========================================================
// LÓGICA DE EJECUCIÓN CON TIMEOUT (Requerimiento PDF)
// =========================================================

func runTaskAsync(task common.Task) {
	defer atomic.AddInt32(&activeTasks, -1)
	
	// Canales para manejar resultado o timeout
	done := make(chan struct{})
	var outputMeta []common.ShuffleMeta
	var err error
	
	startTime := time.Now()

	go func() {
		// Esta llamada bloquea hasta que el Pool tenga espacio y la tarea termine
		outputMeta, err = GlobalExecutor.Submit(task)
		close(done)
	}()

	// Estructura del reporte base
	report := common.TaskReport{
		TaskID:    task.TaskID,
		JobID:     task.JobID,
		StageID:   task.StageID,
		WorkerID:  MyID,
		Timestamp: time.Now().Unix(),
	}

	// SELECT: Esperar terminación O Timeout
	select {
	case <-done:
		duration := time.Since(startTime).Milliseconds()
		report.DurationMs = duration
		
		if err != nil {
			log.Printf("[Worker] Tarea %s FALLÓ tras %dms: %v", task.TaskID, duration, err)
			report.Status = common.TaskStatusFailure
			report.ErrorMsg = err.Error()
		} else {
			log.Printf("[Worker] Tarea %s ÉXITO en %dms", task.TaskID, duration)
			report.Status = common.TaskStatusSuccess
			if task.OutputTarget.Type == common.OutputTypeShuffle {
				report.ShuffleOutput = outputMeta
			} else if len(outputMeta) > 0 {
				report.OutputPath = outputMeta[0].Path
			}
		}

	case <-time.After(TaskTimeout):
		// CASO TIMEOUT (Requerimiento PDF: Terminación si excede tiempo)
		log.Printf("[Worker] Tarea %s EXPIRÓ (Timeout > %s)", task.TaskID, TaskTimeout)
		report.Status = common.TaskStatusFailure
		report.ErrorMsg = fmt.Sprintf("Timeout execution limit exceeded (%s)", TaskTimeout)
		report.DurationMs = time.Since(startTime).Milliseconds()
		// Nota: En Go las goroutines no se pueden "matar" forzosamente desde fuera fácilmente 
		// sin cooperacion, pero al menos reportamos el fallo y liberamos al Master.
	}

	// Enviar reporte
	if repErr := ReportToMaster(report); repErr != nil {
		log.Printf("[Worker] ERROR reportando tarea %s: %v", task.TaskID, repErr)
	}
}

// Variable para Mocking en tests
var ReportToMaster = func(report common.TaskReport) error {
	data, _ := json.Marshal(report)
	resp, err := http.Post(MasterURL+"/report", "application/json", bytes.NewBuffer(data))
	if err != nil { return err }
	defer resp.Body.Close()
	return nil
}

// =========================================================
// HEARTBEAT CON MÉTRICAS REALES (Requerimiento PDF)
// =========================================================

func startHeartbeatLoop() {
	ticker := time.NewTicker(HeartbeatInterval)
	defer ticker.Stop()

	for range ticker.C {
		// 1. Obtener Métricas de Memoria Reales
		var m runtime.MemStats
		runtime.ReadMemStats(&m)
		// Convertir bytes a MB
		memUsageMB := m.Alloc / 1024 / 1024

		hb := common.Heartbeat{
			WorkerID:      MyID,
			Address:       MyID,
			Status:        determineStatus(),
			ActiveTasks:   int(atomic.LoadInt32(&activeTasks)),
			MemUsageMB:    memUsageMB, // Dato real
			LastHeartbeat: time.Now().Unix(),
		}

		data, _ := json.Marshal(hb)
		// Ignoramos error de heartbeat (es best-effort)
		http.Post(MasterURL+"/heartbeat", "application/json", bytes.NewBuffer(data))
	}
}

func determineStatus() string {
	if atomic.LoadInt32(&activeTasks) >= 4 { // Si el pool está lleno
		return common.WorkerStatusBusy
	}
	return common.WorkerStatusIdle
}