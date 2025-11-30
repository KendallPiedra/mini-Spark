package	common

type Heartbeat struct {
	WorkerID   string `json:"worker_id"`
	Address    string `json:"address"`
	Status     string `json:"status"`
	ActiveTasks int    `json:"active_tasks"`
	MemoryUsageMB int    `json:"memory_usage_mb"` // Memoria usada en MB
	LastHeartbeat int64  `json:"last_heartbeat"` // Timestamp del último heartbeat
}