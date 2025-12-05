package common

// --- 1. Definicion de Constantes (Tipos de Operaciones, Origen, Destino y Estado) ---

// Tipos de Operaciones (OperationNode.Type)
const (
	OpTypeMap           = "MAP"
	OpTypeFilter        = "FILTER"
	OpTypeReduceByKey   = "REDUCE_BY_KEY"
	// Agrega más tipos si implementas JOIN o AGGREGATE en fases posteriores


// Tipos de Origen de Datos (TaskInput.SourceType)

	SourceTypeFile    = "FILE"      // Leer de un archivo de entrada (InputPath)
	SourceTypeShuffle = "SHUFFLE"   // Leer de otros workers (a traves de ShuffleMap)
	SourceTypeNone    = "NONE"      // Para tareas que no necesitan entrada (ej. tarea inicial)

// Tipos de Destino de Datos (TaskOutput.Type)
	OutputTypeLocalSpill = "LOCAL_SPILL"  // Escribir en archivo temporal del Worker
	OutputTypeShuffle    = "SHUFFLE"     // Salida particionada (N archivos)
	//OutputTypeFinal      = "FINAL_OUTPUT" // Escribir en el OutputPath final

// Estados de un Worker (Heartbeat.Status)
	WorkerStatusIdle = "IDLE"
	WorkerStatusBusy = "BUSY"

// Estados de una Tarea (TaskReport.Status)
	TaskStatusSuccess      = "SUCCESS"
	TaskStatusFailure      = "FAILURE"
	TaskStatusRetryNeeded = "RETRY_NEEDED"

// Tipos de Destino
)

