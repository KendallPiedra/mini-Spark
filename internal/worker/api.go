package worker

import (
	"bytes" // Necesario para el buffer en http.Post
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	"mini-spark/internal/common"
)

// MasterAddress es la dirección del Master. Usamos una variable (var)
// para que pueda ser modificada en las pruebas de integración.
var MasterAddress = "http://localhost:8080"

// StartServer inicia el servidor HTTP del Worker.
func StartServer(workerID string, port int) {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /tasks", HandleTaskAssignment)
	// NOTA: Si necesitas usar workerID dentro del handler, puedes pasar una closure
	// mux.HandleFunc("POST /tasks", func(w http.ResponseWriter, r *http.Request) {
	//     HandleTaskAssignment(w, r, workerID)
	// })

	addr := fmt.Sprintf(":%d", port)
	log.Printf("[Worker %s] Servidor iniciado en %s", workerID, addr)

	if err := http.ListenAndServe(addr, mux); err != nil {
		log.Fatalf("[Worker %s] Error al iniciar servidor: %v", workerID, err)
	}
}

// HandleTaskAssignment recibe la tarea del Master, la ejecuta y reporta el resultado.
// Es exportada (mayúscula) para ser llamada directamente en tests de integración.
func HandleTaskAssignment(w http.ResponseWriter, r *http.Request) {
	var task common.Task

	// Asumimos un WorkerID estático por ahora, debe ser el real en producción
	const workerID = "WORKER_ID_PENDIENTE" 
	
	// 1. Deserializar la Tarea
	if err := json.NewDecoder(r.Body).Decode(&task); err != nil {
		http.Error(w, "JSON invalido de la tarea", http.StatusBadRequest)
		return
	}

	log.Printf("[Worker %s] Recibida tarea %s de tipo %s", workerID, task.TaskID, task.Operation.Type)

	// 2. Ejecutar la Tarea (Llamada al componente probado)
	// ExecuteTask ahora devuelve una lista de metadatos de archivos
	outputMeta, err := ExecuteTask(task)

	report := common.TaskReport{
		TaskID:    task.TaskID,
		WorkerID:  workerID, 
		Timestamp: time.Now().Unix(),
	}

	if err != nil {
		// 3. Reporte de Falla
		report.Status = common.TaskStatusFailure
		report.ErrorMsg = err.Error()
		log.Printf("[Worker %s] Tarea %s FALLÓ: %v", workerID, task.TaskID, err)
	} else {
		// 3. Reporte de Éxito
		report.Status = common.TaskStatusSuccess
		
		// Llenamos el reporte basado en el tipo de salida
		if task.OutputTarget.Type == common.OutputTypeShuffle {
			// Si es SHUFFLE, reportamos la lista de particiones
			report.ShuffleOutput = outputMeta
		} else if len(outputMeta) > 0 {
			// Si es LOCAL_SPILL, tomamos la ruta del único archivo generado (part_0)
			report.OutputPath = outputMeta[0].Path
		}
		
		log.Printf("[Worker %s] Tarea %s OK. Generados %d archivos.", workerID, task.TaskID, len(outputMeta))
	}

	// 4. Enviar Reporte al Master (ReportToMaster)
	if reportErr := ReportToMaster(report); reportErr != nil {
		// Loguear el error, pero la tarea se ejecutó localmente.
		log.Printf("[Worker %s] ERROR al reportar al Master sobre la tarea %s: %v", workerID, task.TaskID, reportErr)
	}

	// 5. Responder al Master
	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, "Tarea %s recibida y procesando", task.TaskID)
}

// ReportToMaster envia el TaskReport de vuelta al Master.
// Es una VARIABLE de función (var) para poder ser sobrescrita/mockeada en tests.
var ReportToMaster = func(report common.TaskReport) error {
	reportJSON, err := json.Marshal(report)
	if err != nil {
		return err
	}

	// El Master debe tener un endpoint para recibir reportes
	url := fmt.Sprintf("%s/report", MasterAddress) 
	
	resp, err := http.Post(url, "application/json", bytes.NewBuffer(reportJSON))
	if err != nil {
		return fmt.Errorf("no se pudo conectar con el Master en %s: %w", url, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		// Podrías leer el cuerpo para obtener más detalles del error del Master
		return fmt.Errorf("Master devolvio status %d", resp.StatusCode)
	}
	
	return nil
}