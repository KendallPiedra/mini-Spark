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
	// 1. Generar pocos datos (el delay lo hará lento, no necesitamos volumen)
	// Con 100 líneas * 500ms = 50 segundos de proceso. Suficiente para matar el worker.
	content := "gato perro sol luna "
	fullContent := ""
	for i := 0; i < 100; i++ {
		fullContent += content + "\n"
	}
	os.WriteFile("/tmp/input_chaos.txt", []byte(fullContent), 0644)

	// 2. Job usando la UDF LENTA
	job := common.JobRequest{
		Name:       "Chaos-Test",
		InputPath:  "/tmp/input_chaos.txt",
		NumPartitions: 2,
		DAG: common.DAG{
			Nodes: []common.OperationNode{
				{
					ID:            "stage-map",
					Type:          "MAP",
					UDFName:       "map_slow", // <--- AQUÍ ESTÁ EL TRUCO
					NumPartitions: 2,
				},
				{
					ID:            "stage-reduce",
					Type:          "REDUCE_BY_KEY",
					UDFName:       "reduce_sum",
					NumPartitions: 1, // 1 solo reducer para ver el resultado final fácil
				},
			},
			Edges: [][]string{{"stage-map", "stage-reduce"}},
		},
	}

	fmt.Println(" Enviando Job de CAOS (Lento)...")
	data, _ := json.Marshal(job)
	http.Post("http://localhost:8080/api/v1/jobs", "application/json", bytes.NewBuffer(data))
	fmt.Println(" Job enviado. Tienes 50 segundos para matar un worker.")
}