package	common

type Heartbeat struct {
	WorkerID   		string `json:"worker_id"`
	Address    		string `json:"address"`
	Status     		string `json:"status"`
	ActiveTasks 	int    `json:"active_tasks"`
	MemUsageMB 		uint64 `json:"mem_usage_mb"` // Memoria usada en MB
	LastHeartbeat 	int64  `json:"last_heartbeat"` // Timestamp del último heartbeat
}