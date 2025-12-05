package master

import (
    "encoding/json"
    "log"
    "net/http"
    "mini-spark/internal/common"
)

// Este handler debe añadirse a tu Master's mux (ej. en StartServer)
// mux.HandleFunc("POST /report", handleTaskReport)

func handleTaskReport(w http.ResponseWriter, r *http.Request) {
    var report common.TaskReport

    // 1. Deserializar el Reporte
    if err := json.NewDecoder(r.Body).Decode(&report); err != nil {
        http.Error(w, "JSON invalido del reporte", http.StatusBadRequest)
        return
    }

    log.Printf("[Master] Reporte recibido de Worker %s para Tarea %s. Estado: %s", report.WorkerID, report.TaskID, report.Status)

    // 2. Lógica de Actualización de Estado (Pendiente de implementación)
    // Aquí es donde el Master actualiza el JobStatus en su base de datos/memoria.
    
    // Si la tarea falló, se debe re-encolar.
    if report.Status == common.TaskStatusFailure {
        log.Printf("[Master] Iniciando lógica de reintento para Tarea %s", report.TaskID)
        // Ejemplo: Buscar la tarea original y re-encolarla en el Scheduler
        // s.EnqueueTasks([]common.Task{originalTask}) 
    }
    
    // Si la tarea fue exitosa, el Master marca la partición como completada y
    // verifica si la etapa (stage) terminó.

    // 3. Responder al Worker
    w.WriteHeader(http.StatusOK)
    fmt.Fprintf(w, "Reporte %s recibido", report.TaskID)
}