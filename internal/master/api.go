package master

import (
	"encoding/json"
	"fmt"
	"net/http"
	"mini-spark/internal/common"
	"mini-spark/internal/storage"
	"github.com/google/uuid"
	"strings"
)

type MasterServer struct {
	Scheduler *Scheduler
	Registry  *WorkerRegistry
	Store     *storage.JobStore
}

func (s *MasterServer) HandleSubmitJob(w http.ResponseWriter, r *http.Request) {
	var req common.JobRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), 400)
		return
	}
	req.JobID = uuid.New().String()
	s.Store.SaveJob(&req)
	
	fmt.Printf("[Master] Job recibido: %s con %d etapas.\n", req.JobID, len(req.Graph))

	// Iniciar la PRIMERA etapa (Graph[0])
	firstNode := req.Graph[0]
	var tasks []common.Task
	
	// IMPORTANTE: Definir cuántas particiones tendrá la SIGUIENTE etapa (Reduce)
	// para que el Map sepa en cuántos buckets dividir.
	nextPartitions := 1
	if len(req.Graph) > 1 {
		nextPartitions = req.Graph[1].Partitions
	}

	for i := 0; i < firstNode.Partitions; i++ {
		tasks = append(tasks, common.Task{
			TaskID: fmt.Sprintf("%s-%s-%d", req.JobID, firstNode.ID, i),
			JobID: req.JobID,
			StageID: firstNode.ID,
			Operation: firstNode,
			InputPartition: common.TaskInput{
				SourceType: common.SourceTypeFile,
				Path: req.InputPath,
			},
			OutputTarget: common.TaskOutput{
				Type: common.OutputTypeShuffle, // Map siempre hace Shuffle en este modelo
				Path: fmt.Sprintf("/tmp/mini-spark/%s/%s", req.JobID, firstNode.ID),
				Partitions: nextPartitions, // Buckets = particiones del Reduce
			},
		})
	}
	s.Scheduler.SubmitTasks(tasks)
	
	json.NewEncoder(w).Encode(map[string]string{"job_id": req.JobID, "status": "SUBMITTED"})
}

func (s *MasterServer) HandleHeartbeat(w http.ResponseWriter, r *http.Request) {
	var hb common.Heartbeat
	if err := json.NewDecoder(r.Body).Decode(&hb); err != nil { return }
	s.Registry.UpdateHeartbeat(hb)
}

// En /internal/master/api.go

func (s *MasterServer) HandleReport(w http.ResponseWriter, r *http.Request) {
	var rep common.TaskReport
	if err := json.NewDecoder(r.Body).Decode(&rep); err != nil { return }
	
	fmt.Printf("[Master] Reporte: Tarea %s [%s] Worker %s\n", rep.TaskID, rep.Status, rep.WorkerID)
	
	// YA NO NECESITAMOS PARSEAR STRINGS. Usamos los datos explícitos.
	jobID := rep.JobID
	stageID := rep.StageID

	if jobID != "" && stageID != "" {
		s.Store.SaveTaskReport(jobID, stageID, rep)
		
		if rep.Status == common.TaskStatusSuccess {
			// Notificar al Scheduler que una tarea terminó exitosamente
			s.Scheduler.OnTaskCompleted(rep, jobID, stageID)
		}
	} else {
		fmt.Println("[Master] Error: Reporte recibido sin JobID o StageID")
	}
}