package master

import (
	"log"
	"sync"
	"time"
	"mini-spark/internal/common"
)

const WorkerTimeoutSeconds = 10

type WorkerRegistry struct {
	mu      sync.RWMutex
	workers map[string]common.Heartbeat
}

func NewWorkerRegistry() *WorkerRegistry {
	return &WorkerRegistry{
		workers: make(map[string]common.Heartbeat),
	}
}

// UpdateHeartbeat registra o actualiza un worker.
func (r *WorkerRegistry) UpdateHeartbeat(hb common.Heartbeat) {
	r.mu.Lock()
	defer r.mu.Unlock()
	hb.LastHeartbeat = time.Now().Unix()
	
	// Si el worker estaba marcado como DOWN o no existía, loguear reingreso
	if old, exists := r.workers[hb.WorkerID]; !exists || isDead(old) {
		log.Printf("[Registry] Worker %s registrado/recuperado (Address: %s)", hb.WorkerID, hb.Address)
	}
	r.workers[hb.WorkerID] = hb
}

// GetAliveWorkers devuelve workers que han enviado heartbeat recientemente.
// Implementa política básica de "Load Awareness": ordena o filtra por carga (aquí simplificado).
func (r *WorkerRegistry) GetAliveWorkers() []common.Heartbeat {
	r.mu.RLock()
	defer r.mu.RUnlock()
	
	var alive []common.Heartbeat
	now := time.Now().Unix()
	
	for _, w := range r.workers {
		if now - w.LastHeartbeat < WorkerTimeoutSeconds {
			alive = append(alive, w)
		}
	}
	return alive
}

// DetectDeadWorkers identifica workers que han expirado y devuelve sus IDs.
// Usado por el Scheduler para disparar replanificación.
func (r *WorkerRegistry) DetectDeadWorkers() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	
	var deadIDs []string
	now := time.Now().Unix()
	
	for id, w := range r.workers {
		if now - w.LastHeartbeat >= WorkerTimeoutSeconds {
			// Solo reportar si no ha sido ya removido o marcado (opcional: limpiar mapa)
			deadIDs = append(deadIDs, id)
			delete(r.workers, id) // Lo removemos del pool activo
			log.Printf("[Registry] ALERTA: Worker %s declarado MUERTO (Timeout)", id)
		}
	}
	return deadIDs
}

func isDead(w common.Heartbeat) bool {
	return time.Now().Unix() - w.LastHeartbeat >= WorkerTimeoutSeconds
}