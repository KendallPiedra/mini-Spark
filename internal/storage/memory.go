package storage

import (
	"sync"
	"time"
	"mini-spark/internal/common"
)

// JobState mantiene el estado en tiempo de ejecución de un Job
type JobState struct {
	Request      *common.JobRequest
	Status       string
	StartTime    int64
	StageReports map[string][]common.TaskReport // Map[StageID] -> Reports
	TaskStatus   map[string]string            // Map[TaskID] -> Status
}

type JobStore struct {
	mu   sync.RWMutex
	Jobs map[string]*JobState
}

func NewJobStore() *JobStore {
	return &JobStore{
		Jobs: make(map[string]*JobState),
	}
}

func (s *JobStore) CreateJob(req *common.JobRequest) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.Jobs[req.JobID] = &JobState{
		Request:      req,
		Status:       common.JobStatusAccepted,
		StartTime:    time.Now().Unix(),
		StageReports: make(map[string][]common.TaskReport),
		TaskStatus:   make(map[string]string),
	}
}

func (s *JobStore) GetJob(jobID string) *JobState {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.Jobs[jobID]
}

func (s *JobStore) UpdateJobStatus(jobID, status string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if job, ok := s.Jobs[jobID]; ok {
		job.Status = status
	}
}

func (s *JobStore) AddTaskReport(jobID, stageID string, report common.TaskReport) {
	s.mu.Lock()
	defer s.mu.Unlock()
	job, ok := s.Jobs[jobID]
	if !ok { return }

	job.TaskStatus[report.TaskID] = report.Status
	if report.Status == common.TaskStatusSuccess {
		job.StageReports[stageID] = append(job.StageReports[stageID], report)
	}
}

func (s *JobStore) GetStageReports(jobID, stageID string) []common.TaskReport {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if job, ok := s.Jobs[jobID]; ok {
		return job.StageReports[stageID]
	}
	return nil
}