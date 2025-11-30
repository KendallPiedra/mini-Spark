package common

type Task struct {
	TaskID     string `json:"task_id"`
	JobID      string `json:"job_id"`
	StageID    string `json:"stage_id"`
	Operation OperationNode `json:"operation"`   // Operación a realizar en esta tarea
	InputPartition TaskInput `json:"input_partition"` // Particion de entrada para esta tarea
	OutputTarget TaskOutput `json:"output_target"`   // Destino de salida para esta tarea
}

type TaskInput struct {
	SourceType string `json:"source_type"` // "file" o "shuffle"
	Path 	 string `json:"path"`        // Ruta del archivo o ubicación del shuffle
	Offsets   [2]int64  `json:"offsets"`      // rango de bytes a leer (start, end)
	ShuffleMap map[string]string `json:"shuffle_map"` // Mapa de WorkerID a URL para descargar datos de Shuffle (si SourceType=SHUFFLE)

}

type TaskOutput struct {
	Type		  string `json:"type"` // "file" o "shuffle"
	Path 		   string `json:"path"`             // Ruta del archivo o ubicación del shuffle
	WorkerID      string `json:"worker_id"`       // ID del shuffle (si DestinationType=SHUFFLE)
}