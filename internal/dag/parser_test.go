package dag_test

import (
	"testing"
	"mini-spark/internal/common"
	"mini-spark/internal/dag"
	"github.com/google/uuid"
)

// setupJob crea una estructura JobRequest simple para usar en las pruebas
func setupJob(partitions int, opType string) common.JobRequest {
	return common.JobRequest{
		JobID: "job-" + uuid.New().String(),
		InputPath: "/data/input/test.csv",
		Graph: []common.OperationNode{
			{
				ID: "map-stage-1",
				Type: opType,
				UDFName: "test_map_func",
				Dependencies: nil,
				Partitions: partitions,
			},
		},
	}
}

func TestGenerateMapTasks(t *testing.T) {
	// Estructura de la tabla de pruebas
	tests := []struct {
		name           string
		partitions     int
		opType         string
		expectError    bool
		expectedCount  int
	}{
		{name: "Tareas Map normales (5)", partitions: 5, opType: common.OpTypeMap, expectError: false, expectedCount: 5},
		{name: "Tareas Filter (1)", partitions: 1, opType: common.OpTypeFilter, expectError: false, expectedCount: 1},
		{name: "Cero Particiones (Error)", partitions: 0, opType: common.OpTypeMap, expectError: true, expectedCount: 0},
		{name: "Tipo no Soportado (Error)", partitions: 2, opType: common.OpTypeReduceByKey, expectError: true, expectedCount: 0},
		{name: "Muchas Particiones (10)", partitions: 10, opType: common.OpTypeMap, expectError: false, expectedCount: 10},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			job := setupJob(tt.partitions, tt.opType)
			node := job.Graph[0]

			tasks, err := dag.GenerateMapTasks(job, node)

			if (err != nil) != tt.expectError {
				t.Fatalf("Se esperaba error=%t, pero se obtuvo error: %v", tt.expectError, err)
			}

			if !tt.expectError && len(tasks) != tt.expectedCount {
				t.Errorf("Cuenta de tareas incorrecta. Esperado %d, Obtenido %d", tt.expectedCount, len(tasks))
			}

			if len(tasks) > 0 {
				// Verifica la unicidad de IDs solo si no hubo error y se generaron tareas
				taskIDs := make(map[string]bool)
				for _, task := range tasks {
					if taskIDs[task.TaskID] {
						t.Errorf("Error de unicidad: TaskID duplicado encontrado: %s", task.TaskID)
					}
					taskIDs[task.TaskID] = true
				}
			}
		})
	}
}