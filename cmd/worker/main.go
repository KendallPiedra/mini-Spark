package main

import (
	"flag"
	"mini-spark/internal/worker"
)
// Se ejecuta el worker
func main() {
	// se definen los flags
	port := flag.Int("port", 8081, "Puerto del worker")
	master := flag.String("master", "http://localhost:8080", "URL del Master")
	flag.Parse()

	worker.StartServer(*port, *master)
}