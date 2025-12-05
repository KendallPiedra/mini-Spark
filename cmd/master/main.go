package main

import (
	"log"
	"net/http"
	"mini-spark/internal/master"
	"mini-spark/internal/storage"
)

func main() {
	store := storage.NewJobStore()
	registry := master.NewWorkerRegistry()
	scheduler := master.NewScheduler(registry, store)
	scheduler.Start()

	server := &master.MasterServer{
		Scheduler: scheduler,
		Registry:  registry,
		Store:     store,
	}

	http.HandleFunc("/submit", server.HandleSubmitJob)
	http.HandleFunc("/heartbeat", server.HandleHeartbeat)
	http.HandleFunc("/report", server.HandleReport)

	log.Println("Master iniciado en :8080")
	log.Fatal(http.ListenAndServe(":8080", nil))
}