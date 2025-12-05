package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"

	"mini-spark/internal/common"
)

func main() {
	masterURL := "http://localhost:8080"

	// 1. Generar datos de prueba
	createDummyData("/tmp/input_big.txt")

	// 2. Definir el Job con estructura DAG
	job := common.JobRequest{
		Name:       "WordCount-DAG",
		InputPath:  "/tmp/input_big.txt",
		NumPartitions: 2, // Paralelismo global
		DAG: common.DAG{
			Nodes: []common.OperationNode{
				// Nodo 0: Map (Lectura y Tokenización)
				{
					ID:         "stage-map",
					Type:       common.OpTypeMap, // "MAP"
					UDFName:    "map_wordcount",
					NumPartitions: 2, // 2 Workers leyendo
				},
				// Nodo 1: Reduce (Suma por clave)
				{
					ID:         "stage-reduce",
					Type:       common.OpTypeReduceByKey, // "REDUCE_BY_KEY"
					UDFName:    "reduce_sum",
					NumPartitions: 2, // 2 Workers reduciendo
				},
			},
			// Definimos el flujo de datos: Map -> Reduce
			Edges: [][]string{
				{"stage-map", "stage-reduce"},
			},
		},
	}

	// 3. Enviar el Job
	fmt.Println("Enviando Job al Master...")
	jobID := submitJob(masterURL, job)
	fmt.Printf("Job aceptado con ID: %s\n", jobID)

	// 4. Polling de estado
	monitorJob(masterURL, jobID)
}

func submitJob(baseURL string, job common.JobRequest) string {
	data, _ := json.Marshal(job)
	resp, err := http.Post(baseURL+"/api/v1/jobs", "application/json", bytes.NewBuffer(data))
	if err != nil {
		panic(fmt.Sprintf("Error contactando master: %v", err))
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		body, _ := io.ReadAll(resp.Body)
		panic(fmt.Sprintf("Master rechazó job: %s", string(body)))
	}

	var res map[string]string
	json.NewDecoder(resp.Body).Decode(&res)
	return res["job_id"]
}

func monitorJob(baseURL, jobID string) {
	for {
		resp, err := http.Get(fmt.Sprintf("%s/api/v1/jobs/%s", baseURL, jobID))
		if err != nil {
			fmt.Printf("Error consultando estado: %v\n", err)
			break
		}
		
		var status map[string]interface{}
		json.NewDecoder(resp.Body).Decode(&status)
		resp.Body.Close()

		currentState := status["Status"].(string)
		fmt.Printf("Estado del Job: %s\n", currentState)

		if currentState == "SUCCEEDED" || currentState == "FAILED" {
			break
		}
		time.Sleep(1 * time.Second)
	}
}

func createDummyData(path string) {
	content := "gato perro gato raton perro gato elefante nube nube sol gato"
	// Multiplicamos para tener algo de volumen
	fullContent := ""
	for i := 0; i < 500; i++ {
		fullContent += content + "\n"
	}
	os.WriteFile(path, []byte(fullContent), 0644)
	fmt.Printf("Archivo de entrada creado en %s\n", path)
}