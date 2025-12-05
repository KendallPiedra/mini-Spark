package dag

import (
	"fmt"
	"github.com/google/uuid"
	"mini-spark/internal/common" // Asumimos que esta ruta contiene los archivos .go de los structs
)

// GenerateMapTasks crea N tareas (Tasks) a partir de una OperationNode,
// dividiendo el archivo de entrada.
func GenerateMapTasks(job common.JobRequest, node common.OperationNode) ([]common.Task, error) {
	// Usamos las constantes definidas en el paquete common (si están en otro archivo como 'types.go')
	if node.Type != common.OpTypeMap && node.Type != common.OpTypeFilter {
		return nil, fmt.Errorf("tipo de operacion no soportada para la fase 2: %s", node.Type)
	}

	totalTasks := node.NumPartitions
	if totalTasks <= 0 {
		return nil, fmt.Errorf("el numero de particiones debe ser mayor a cero")
	}

	tasks := make([]common.Task, totalTasks)

	for i := 0; i < totalTasks; i++ {
		tasks[i] = common.Task{
			TaskID:  uuid.New().String(),
			JobID:   job.JobID,
			StageID: node.ID,
			Operation: node,
			InputPartition: common.TaskInput{
				SourceType: common.SourceTypeFile, // Constante del archivo de tipos
				Path:       job.InputPath,
				Offsets:    [2]int64{0, 0}, 
			},
			OutputTarget: common.TaskOutput{
				Type: common.OutputTypeLocalSpill, // Constante del archivo de tipos
				Path: fmt.Sprintf("/tmp/mini-spark/%s_task_%d_out", job.JobID, i),
			},
		}
	}

	return tasks, nil
}