package common
type TaskReport struct {
	TaskID      string `json:"task_id"`
	WorkerID	string `json:"worker_id"`
	Status	  string `json:"status"`       // "completed", "failed", "in_progress"
	ErrorMessage string `json:"error_message"`
	DurationMS   int64  `json:"duration_ms"`  // Duración de la tarea en milisegundos
	ShuffleOutput []ShuffleMeta `json:"shuffle_outputs"` // Metadatos de salidas de shuffle generadas
	Timestamp	int64  `json:"timestamp"`    // Un entero que representa segundos para facilitar el ordenamiento
	OutputPath  string `json:"output_path"` // Ruta del archivo de salida !!!Para pruebas

}

type ShuffleMeta struct {
	PartitionKey string `json:"partition_key"` // La clave de partición (ej: "part_0_of_4")
	LocationURL  string `json:"location_url"`  // URL en el Worker para que otro Worker lo descargue (ej: "http://worker-id:8081/data/...")
	SizeKB       uint64 `json:"size_kb"`       // Tamaño del dato para optimización
}