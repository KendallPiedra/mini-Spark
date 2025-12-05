package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"

	"mini-spark/internal/common"
)

// Configuración
var (
	masterURL  string
	submitFile string
	jobIDArg   string
	poll       bool
)

func main() {
	// Definir argumentos de línea de comandos
	flag.StringVar(&masterURL, "master", "http://localhost:8080", "URL del Master")
	flag.StringVar(&submitFile, "submit", "", "Ruta al archivo JSON con la definición del Job")
	flag.StringVar(&jobIDArg, "status", "", "Consultar estado de un Job ID específico")
	flag.BoolVar(&poll, "watch", false, "Si se usa con -submit o -status, se queda monitoreando hasta finalizar")
	flag.Parse()

	// MODO 1: Consultar Estado
	if jobIDArg != "" {
		checkStatus(jobIDArg)
		if poll {
			monitorJob(jobIDArg)
		}
		return
	}

	// MODO 2: Enviar Job
	if submitFile != "" {
		// Leer el archivo JSON
		jsonBytes, err := os.ReadFile(submitFile)
		if err != nil {
			panic(fmt.Sprintf("No se pudo leer el archivo %s: %v", submitFile, err))
		}

		// Validar que sea un JSON válido (opcional, pero recomendado)
		var jobReq common.JobRequest
		if err := json.Unmarshal(jsonBytes, &jobReq); err != nil {
			panic(fmt.Sprintf("El archivo no es un JobRequest válido: %v", err))
		}

		fmt.Printf(" Enviando Job: %s (desde %s)\n", jobReq.Name, submitFile)
		jobID := submitJob(jsonBytes)
		fmt.Printf(" Job aceptado con ID: %s\n", jobID)

		if poll {
			monitorJob(jobID)
		}
		return
	}

	// Si no hay argumentos
	fmt.Println("Uso del Cliente:")
	fmt.Println("  Enviar Job:      go run cmd/client/main.go -submit jobs_specs/wordcount.json -watch")
	fmt.Println("  Consultar Job:   go run cmd/client/main.go -status <JOB_ID>")
	flag.PrintDefaults()
}

func submitJob(data []byte) string {
	resp, err := http.Post(masterURL+"/api/v1/jobs", "application/json", bytes.NewBuffer(data))
	if err != nil {
		panic(fmt.Sprintf("Error contactando master: %v", err))
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		panic(fmt.Sprintf("Master rechazó job: %s", string(body)))
	}

	var res map[string]string
	json.NewDecoder(resp.Body).Decode(&res)
	return res["job_id"]
}

func checkStatus(id string) {
	resp, err := http.Get(fmt.Sprintf("%s/api/v1/jobs/%s", masterURL, id))
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}
	defer resp.Body.Close()
	io.Copy(os.Stdout, resp.Body)
	fmt.Println() // Salto de línea
}

func monitorJob(jobID string) {
	fmt.Println(" Monitoreando...")
	for {
		resp, err := http.Get(fmt.Sprintf("%s/api/v1/jobs/%s", masterURL, jobID))
		if err != nil { break }
		
		var status map[string]interface{}
		json.NewDecoder(resp.Body).Decode(&status)
		resp.Body.Close()

		st := status["Status"].(string)
		fmt.Printf("\r>> Estado: %s   ", st) // \r para sobrescribir línea

		if st == "SUCCEEDED" || st == "FAILED" {
			fmt.Println("\n Finalizado.")
			break
		}
		time.Sleep(1 * time.Second)
	}
}