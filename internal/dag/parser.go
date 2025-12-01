package dag

import (
	"fmt"
	"github.com/google/uuid"
	"my-mini-spark/internal/common" 
)

// GenerateMapTasks crea N tareas (Tasks) a partir de una OperationNode.
// Divide el archivo de entrada.
func GenerateMapTasks(job *common.JobRequest, node common.OperationNode) ([]common.Task, error) {
	if node.Type != "MAP" && node.Type != "FILTER" {
		return nil, fmt.Errorf("tipo de operacion no soportada para la fase 2: %s", node.Type)
	}

	totalTasks := node.Partitions
	if totalTasks <= 0 {
		return nil, fmt.Errorf("el numero de particiones debe ser mayor a cero")
	}

	// Se asume que cada tarea procesa el archivo completo. 

	tasks := make([]common.Task, totalTasks)

	for i := 0; i < totalTasks; i++ {
		tasks[i] = common.Task{
			TaskID:  uuid.New().String(),
			JobID:   job.JobID,
			StageID: node.ID,
			Operation: node,
			InputPartition: common.TaskInput{
				SourceType: common.SourceTypeFile, // "FILE"
				Path:       job.InputPath,
				Offsets:    [2]int64{0, 0}, // Placeholder para la particion
				// ShuffleMap queda vacío.
			},
			OutputTarget: common.TaskOutput{
				Type: common.OutputTypeLocalSpill, // "LOCAL_SPILL" (guardar temporalmente)
				Path: fmt.Sprintf("/tmp/mini-spark/%s_task_%d_out", job.JobID, i),
			},
		}
	}

	return tasks, nil
}