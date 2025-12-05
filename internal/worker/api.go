package worker

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"
	"bytes"
	"mini-spark/internal/common"
)

// MasterAddress debe ser configurado al iniciar el Worker.
// Por ahora, asumiremos que el Master corre en este puerto.
var MasterAddress = "http://localhost:8080"


// StartServer inicia el servidor HTTP del Worker.
func StartServer(port int) {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /tasks", HandleTaskAssignment)
	// mux.HandleFunc("GET /status", handleStatus) // Se puede añadir luego para monitoreo

	addr := fmt.Sprintf(":%d", port)
	log.Printf("[Worker] Servidor iniciado en %s", addr)
	
	// Nota: Los workers deben correr en puertos diferentes (8081, 8082, etc.)
	if err := http.ListenAndServe(addr, mux); err != nil {
		log.Fatalf("[Worker] Error al iniciar servidor: %v", err)
	}
}

// handleTaskAssignment recibe la tarea del Master, la ejecuta y reporta el resultado.
func HandleTaskAssignment(w http.ResponseWriter, r *http.Request) {
	var task common.Task
	
	// 1. Deserializar la Tarea
	if err := json.NewDecoder(r.Body).Decode(&task); err != nil {
		http.Error(w, "JSON invalido de la tarea", http.StatusBadRequest)
		return
	}
	
	log.Printf("[Worker %s] Recibida tarea %s de tipo %s", "ID_DEL_WORKER", task.TaskID, task.Operation.Type)

	// 2. Ejecutar la Tarea (Llamada al componente probado)
	outputMeta, err := ExecuteTask(task)
	
	report := common.TaskReport{
		TaskID: task.TaskID,
		WorkerID: "ID_DEL_WORKER", // Reemplazar con el ID real del Worker
		Timestamp: time.Now().Unix(),
	}

	if err != nil {
		// 3. Reporte de Falla
		report.Status = common.TaskStatusFailure
		report.ErrorMessage = err.Error()
		log.Printf("[Worker %s] Tarea %s FALLÓ: %v", report.WorkerID, task.TaskID, err)
	} else {
		// 3. Reporte de Éxito
		if task.OutputTarget.Type == common.OutputTypeShuffle {
			report.ShuffleOutput = outputMeta
		} else if len(outputMeta) > 0 {
			// Para salidas simples, retornamos la ruta del archivo generado
			report.OutputPath = outputMeta[0].Path
		}
		log.Printf("[Worker] Tarea %s OK. Generados %d archivos.", task.TaskID, len(outputMeta))
	}

	// 4. Enviar Reporte al Master (ReportToMaster)
	if reportErr := ReportToMaster(report); reportErr != nil {
		// Esta falla es crítica, pero no bloquea la respuesta al Master.
		log.Printf("[Worker %s] ERROR al reportar al Master sobre la tarea %s: %v", report.WorkerID, task.TaskID, reportErr)
	}

	// 5. Responder al Master
	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, "Tarea %s recibida y procesando", task.TaskID)
}
/*
// ReportToMaster envia el TaskReport de vuelta al Master.
func ReportToMaster(report common.TaskReport) error {
	reportJSON, err := json.Marshal(report)
	if err != nil {
		return err
	}

	// El Master debe tener un endpoint para recibir reportes
	url := fmt.Sprintf("%s/report", MasterAddress) 
	
	resp, err := http.Post(url, "application/json", bytes.NewBuffer(reportJSON))
	if err != nil {
		return fmt.Errorf("no se pudo conectar con el Master: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("Master devolvio status %d", resp.StatusCode)
	}
	
	return nil
}
*/

var ReportToMaster = func(report common.TaskReport) error {
    reportJSON, err := json.Marshal(report)
    if err != nil {
        return err
    }

    // Nota: MasterAddress debe ser accesible (variable o constante exportada)
    url := fmt.Sprintf("%s/report", MasterAddress) 
    
    resp, err := http.Post(url, "application/json", bytes.NewBuffer(reportJSON))
    if err != nil {
        return fmt.Errorf("no se pudo conectar con el Master: %w", err)
    }
    defer resp.Body.Close()

    if resp.StatusCode != http.StatusOK {
        return fmt.Errorf("Master devolvio status %d", resp.StatusCode)
    }
    
    return nil
}

