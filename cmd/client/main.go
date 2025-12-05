package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"mini-spark/internal/common"
)

func main() {
	// 1. Crear archivo de entrada grande
	content := "gato perro gato raton perro gato elefante nube nube sol gato"
	// Repetir contenido para tener volumen
	for i := 0; i < 100; i++ {
		content += " gato perro"
	}
	os.WriteFile("/tmp/input_big.txt", []byte(content), 0644)

	// 2. Definir el Job con 2 Etapas
	job := common.JobRequest{
		Name:      "FullWordCount",
		InputPath: "/tmp/input_big.txt",
		OutputPath: "/tmp/mini-spark-output",
		Graph: []common.OperationNode{
			// Etapa 1: Map
			{
				ID: "stage-map", 
				Type: "MAP", 
				UDFName: "map_wordcount", 
				Partitions: 2, // 2 Workers leyendo el archivo (simulado)
			},
			// Etapa 2: Reduce
			{
				ID: "stage-reduce", 
				Type: "REDUCE_BY_KEY", 
				UDFName: "reduce_sum", 
				Partitions: 2, // 2 Workers reduciendo (2 archivos de salida final)
				Dependencies: []string{"stage-map"},
			},
		},
	}

	data, _ := json.Marshal(job)
	resp, err := http.Post("http://localhost:8080/submit", "application/json", bytes.NewBuffer(data))
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()
	fmt.Println("Job enviado. Mira los logs del Master y Workers.")
}