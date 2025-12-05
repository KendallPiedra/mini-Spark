package main

import (
	"log"
	"net/http"
	"mini-spark/internal/master"
	"mini-spark/internal/storage"
)

func main() {
	// 1. Inicializar Componentes del Master
	store := storage.NewJobStore()
	registry := master.NewWorkerRegistry()
	scheduler := master.NewScheduler(registry, store)

	server := &master.MasterServer{
		Scheduler: scheduler,
		Registry:  registry,
		Store:     store,
	}

	// 2. Definir Rutas (API RESTful + Internas)
	mux := http.NewServeMux()
	
	// API Cliente (Para recibir Jobs)
	mux.HandleFunc("/api/v1/jobs", server.HandleSubmitJob)      
	mux.HandleFunc("/api/v1/jobs/", server.HandleGetJob)        

	// API Interna (Comunicación Worker -> Master)
	mux.HandleFunc("/heartbeat", server.HandleHeartbeat)
	mux.HandleFunc("/report", server.HandleReport)

	log.Println(" Master iniciado en puerto :8080")
	log.Println("   - Esperando workers...")
	
	// 3. Bloquear y escuchar
	if err := http.ListenAndServe(":8080", mux); err != nil {
		log.Fatal(err)
	}
}