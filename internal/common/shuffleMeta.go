package common

// ShuffleMeta contiene la metadata de un archivo generado durante la fase de Shuffle.
type ShuffleMeta struct {
	PartitionKey int    `json:"partition_key"` // ID de la partición (0 a N-1)
	Path         string `json:"path"`          // Ruta local donde está el archivo
	Size         int64  `json:"size"`          // Tamaño en bytes
}