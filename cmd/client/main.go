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

// URL del Master
const MasterURL = "http://localhost:8080"

func main() {
	fmt.Println("--- INICIANDO CLIENTE MINI-SPARK (JOIN TEST) ---")

	// 1. Generar datos de prueba (Simulando dos tablas: Usuarios y Órdenes)
	inputPath := "/tmp/input_join.txt"
	createJoinData(inputPath)

	// 2. Definir el Job con estructura DAG para un JOIN
	job := common.JobRequest{
		Name:       "Join-Users-Orders",
		InputPath:  inputPath,
		NumPartitions: 2, // Paralelismo global
		DAG: common.DAG{
			Nodes: []common.OperationNode{
				// NODO 1: MAP
				// Lee el archivo mixto y etiqueta: "L:" para usuarios, "R:" para órdenes.
				{
					ID:            "stage-map-parse",
					Type:          common.OpTypeMap, 
					UDFName:       "map_parse_tables", 
					NumPartitions: 2,
				},
				// NODO 2: JOIN
				// Recibe datos barajados (Shuffle) por ID y cruza L con R.
				{
					ID:            "stage-join",
					Type:          common.OpTypeJoin, 
					UDFName:       "join_users_orders",
					NumPartitions: 2,
				},
			},
			// Definimos la dependencia: Map -> Join
			Edges: [][]string{
				{"stage-map-parse", "stage-join"},
			},
		},
	}

	// 3. Enviar el Job al Master
	fmt.Println("🚀 Enviando Job al Master...")
	jobID := submitJob(job)
	fmt.Printf("✅ Job aceptado con ID: %s\n", jobID)

	// 4. Monitorear el estado hasta que termine
	monitorJob(jobID)
}

// --- FUNCIONES AUXILIARES ---

func createJoinData(path string) {
	// Generamos un archivo CSV mixto
	// Formato Usuarios: U,ID,Nombre
	// Formato Órdenes:  O,OrderID,UserID,Producto
	content := ""
	
	// Usuarios
	content += "U,1,Juan\n"
	content += "U,2,Maria\n"
	content += "U,3,Pedro\n"
	content += "U,4,Ana\n"
	
	// Pedidos
	content += "O,100,1,Laptop\n"   // Juan compró Laptop
	content += "O,101,1,Mouse\n"    // Juan compró Mouse
	content += "O,102,2,Monitor\n"  // Maria compró Monitor
	content += "O,103,4,Teclado\n"  // Ana compró Teclado
	content += "O,104,4,Webcam\n"   // Ana compró Webcam
	// Pedro no compró nada (no aparecerá en el Join)

	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		panic(fmt.Sprintf("Error creando archivo de datos: %v", err))
	}
	fmt.Printf("📝 Datos de prueba creados en %s\n", path)
}

func submitJob(job common.JobRequest) string {
	data, _ := json.Marshal(job)
	resp, err := http.Post(MasterURL+"/api/v1/jobs", "application/json", bytes.NewBuffer(data))
	if err != nil {
		panic(fmt.Sprintf("Error contactando master: %v. ¿Está encendido?", err))
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		panic(fmt.Sprintf("Master rechazó job (%d): %s", resp.StatusCode, string(body)))
	}

	var res map[string]string
	if err := json.NewDecoder(resp.Body).Decode(&res); err != nil {
		panic("Error decodificando respuesta del Master")
	}
	return res["job_id"]
}

func monitorJob(jobID string) {
	fmt.Println("⏳ Monitoreando estado del Job...")
	for {
		resp, err := http.Get(fmt.Sprintf("%s/api/v1/jobs/%s", MasterURL, jobID))
		if err != nil {
			fmt.Printf("⚠️ Error consultando estado: %v\n", err)
			break
		}
		
		var status map[string]interface{}
		if err := json.NewDecoder(resp.Body).Decode(&status); err != nil {
			fmt.Println("Error leyendo respuesta de estado")
			resp.Body.Close()
			break
		}
		resp.Body.Close()

		// Extraer estado, manejando posibles nulos
		stateInterface := status["Status"]
		if stateInterface == nil {
			fmt.Println("Estado desconocido recibido del Master")
			break
		}
		currentState := stateInterface.(string)
		
		fmt.Printf(">> Estado actual: %s\n", currentState)

		if currentState == "SUCCEEDED" || currentState == "FAILED" {
			fmt.Println("🏁 Ejecución finalizada.")
			break
		}
		
		// Esperar un poco antes de volver a consultar
		time.Sleep(1 * time.Second)
	}
}