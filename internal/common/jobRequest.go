package common

type JobRequest struct {
	JobID      string `json:"job_id"` // Generado por el sistema si viene vacío
	Name       string `json:"name"`
	InputPath  string `json:"path"`
	NumPartitions int    `json:"partitions"`
	DAG        DAG    `json:"dag"`
}