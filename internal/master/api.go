package master

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"mini-spark/internal/common"
	"mini-spark/internal/storage"
	"github.com/google/uuid"
)

type MasterServer struct {
	Scheduler *Scheduler
	Registry  *WorkerRegistry
	Store     *storage.JobStore
}

// POST /api/v1/jobs
func (s *MasterServer) HandleSubmitJob(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" { http.Error(w, "Method not allowed", 405); return }

	var req common.JobRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid JSON: "+err.Error(), 400); return
	}

	if req.JobID == "" { req.JobID = uuid.New().String() }
	// Si no se especifica particiones globales, usamos un default razonable
	if req.NumPartitions == 0 { req.NumPartitions = 2 }

	s.Store.CreateJob(&req)
	s.Scheduler.SubmitJob(&req)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{
		"job_id": req.JobID,
		"status": common.JobStatusAccepted,
	})
}

// GET /api/v1/jobs/{id}
func (s *MasterServer) HandleGetJob(w http.ResponseWriter, r *http.Request) {
	parts := strings.Split(r.URL.Path, "/")
	// Esperamos /api/v1/jobs/{id}, el ID debería ser el último elemento
	if len(parts) == 0 { http.Error(w, "Bad URL", 400); return }
	jobID := parts[len(parts)-1]

	job := s.Store.GetJob(jobID)
	if job == nil { http.Error(w, "Job not found", 404); return }

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(job)
}

// POST /heartbeat (Internal)
func (s *MasterServer) HandleHeartbeat(w http.ResponseWriter, r *http.Request) {
	var hb common.Heartbeat
	if err := json.NewDecoder(r.Body).Decode(&hb); err != nil { return }
	s.Registry.UpdateHeartbeat(hb)
}

// POST /report (Internal)
func (s *MasterServer) HandleReport(w http.ResponseWriter, r *http.Request) {
	var rep common.TaskReport
	if err := json.NewDecoder(r.Body).Decode(&rep); err != nil { return }

	// Persistir reporte para trazabilidad
	s.Store.AddTaskReport(rep.JobID, rep.StageID, rep)
	
	if rep.Status == common.TaskStatusSuccess {
		// Notificar éxito al Scheduler para que avance el DAG
		s.Scheduler.HandleTaskCompletion(rep)
	} else {
		// MANEJO DE FALLOS
		// Buscamos la tarea real en memoria del Scheduler para re-encolarla.
		// Es vital usar la tarea original porque contiene la definición de la operación (UDF, Inputs).
		s.Scheduler.mu.Lock()
		realTask, exists := s.Scheduler.RunningTasks[rep.TaskID]
		s.Scheduler.mu.Unlock()
		
		if exists {
			s.Scheduler.HandleTaskFailure(realTask, rep.ErrorMsg)
		} else {
			// Si no existe, es posible que sea un reporte tardío de una tarea que ya dimos por perdida,
			// o que el Master se reinició. En este diseño simple, solo logueamos.
			fmt.Printf("[Master] ALERTA: Reporte de fallo para tarea desconocida (posible timeout previo): %s\n", rep.TaskID)
		}
	}
}