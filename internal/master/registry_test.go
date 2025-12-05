package master

import (
	"testing"
	"time"
	"mini-spark/internal/common"
)

func TestWorkerRegistry_HeartbeatAndDetection(t *testing.T) {
	r := NewWorkerRegistry()
	
	workerID := "worker-1"
	address := "localhost:8081"
	
	// 1. Primer Heartbeat
	t.Run("UpdateHeartbeat_Initial", func(t *testing.T) {
		r.UpdateHeartbeat(common.Heartbeat{
			WorkerID: workerID,
			Address:  address,
			Status:   common.WorkerStatusIdle,
		})

		alive := r.GetAliveWorkers()
		if len(alive) != 1 {
			t.Errorf("Esperaba 1 worker vivo, obtuvo %d", len(alive))
		}
	})

	// 2. Detección de Workers Muertos (No debería encontrar ninguno)
	t.Run("DetectDeadWorkers_None", func(t *testing.T) {
		dead := r.DetectDeadWorkers()
		if len(dead) != 0 {
			t.Errorf("No debería haber workers muertos, encontró %d", len(dead))
		}
	})

	// 3. Simular Timeout (Forzar el timeout para el worker expirado)
	t.Run("DetectDeadWorkers_Timeout", func(t *testing.T) {
		expiredID := "worker-expired"
		r.UpdateHeartbeat(common.Heartbeat{
			WorkerID: expiredID,
			Address:  "localhost:8083",
			Status:   common.WorkerStatusIdle,
			// Establecer LastHeartbeat en el pasado para forzar el timeout
			LastHeartbeat: time.Now().Unix() - WorkerTimeoutSeconds - 1,
		})
		
		// Actualizar el worker vivo para que no muera
		r.UpdateHeartbeat(common.Heartbeat{WorkerID: workerID, Address: address, Status: common.WorkerStatusIdle}) 
		
		dead := r.DetectDeadWorkers()
		if len(dead) != 1 || dead[0] != expiredID {
			t.Fatalf("Esperaba que solo %s muriera, obtuvo: %v", expiredID, dead)
		}

		// Verificar que el worker muerto fue removido del registro de vivos
		alive := r.GetAliveWorkers()
		found := false
		for _, w := range alive {
			if w.WorkerID == expiredID { found = true }
		}
		if found {
			t.Errorf("El worker expirado no fue removido del registro.")
		}
	})
}