package common
type TaskReport struct {
	TaskID      	string 		`json:"task_id"`
	WorkerID		string 		`json:"worker_id"`
	Status	  		string 		`json:"status"`       // "completed", "failed", "in_progress"
	ErrorMsg 		string 		`json:"error_msg"`
	OutputPath  	string 		`json:"output_path"` // Ruta del archivo de salida !!!Para pruebas
	Timestamp		int64  		`json:"timestamp"`    // Un entero que representa segundos para facilitar el ordenamiento
	DurationMS 		int64  		`json:"duration_ms"`  // Duración de la tarea en milisegundos
	ShuffleOutput 	[]ShuffleMeta 	`json:"shuffle_outputs"` // Metadatos de salidas de shuffle generadas

}

type ShuffleMeta struct {
	PartitionKey int 		`json:"partition_key"` // La clave de partición (ej: "part_0_of_4")
	Path         string 	`json:"path"`          // Ruta local donde está el archivo
	Size       	 int64 		`json:"size"`       // Tamaño del dato para optimización
	//LocationURL  string 	`json:"location_url"`  // URL en el Worker para que otro Worker lo descargue (ej: "http://worker-id:8081/data/...")
}

