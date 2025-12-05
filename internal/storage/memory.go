package storage

import (
	"sync"
	"mini-spark/internal/common"
)

type JobStore struct {
	mu           sync.RWMutex
	Jobs         map[string]*common.JobRequest
	TaskReports  map[string]common.TaskReport // TaskID -> Report
	StageReports map[string][]common.TaskReport // StageID -> Lista de reportes
}

func NewJobStore() *JobStore {
	return &JobStore{
		Jobs:         make(map[string]*common.JobRequest),
		TaskReports:  make(map[string]common.TaskReport),
		StageReports: make(map[string][]common.TaskReport),
	}
}

func (s *JobStore) SaveJob(job *common.JobRequest) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.Jobs[job.JobID] = job
}

func (s *JobStore) GetJob(jobID string) *common.JobRequest {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.Jobs[jobID]
}

func (s *JobStore) SaveTaskReport(jobID, stageID string, report common.TaskReport) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.TaskReports[report.TaskID] = report
	if report.Status == common.TaskStatusSuccess {
		s.StageReports[stageID] = append(s.StageReports[stageID], report)
	}
}

// CheckStageComplete verifica si todas las tareas de una etapa han terminado exitosamente
func (s *JobStore) CheckStageComplete(stageID string, expectedTasks int) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	reports, exists := s.StageReports[stageID]
	if !exists { return false }
	return len(reports) == expectedTasks
}

func (s *JobStore) GetStageResults(stageID string) []common.TaskReport {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.StageReports[stageID]
}