package main

import (
	"encoding/json"
	"flag"
	"log"
	"strings"
	"time"

	"mini-spark/internal/common"
	"mini-spark/internal/udf"
	"mini-spark/internal/worker"
)

func main() {
	// --- INYECCIÓN DE CÓDIGO DE PRUEBA ---
	// Registramos una UDF "lenta" sin tocar el código base
	log.Println("INICIANDO WORKER EN MODO DE PRUEBA (CON DELAY)")
	
	udf.UDFRegistry["map_slow"] = udf.UDFMapFn(func(r udf.Record) []udf.Record {
		// Simular carga pesada (500ms por registro para darte tiempo de matarlo)
		time.Sleep(500 * time.Millisecond)

		// Lógica original de wordcount
		clean := strings.Map(func(r rune) rune {
			if strings.ContainsRune(".,;?!-", r) { return -1 }
			return r
		}, string(r))
		words := strings.Fields(clean)
		var results []udf.Record
		for _, w := range words {
			kv := common.KeyValue{Key: strings.ToLower(w), Value: "1"}
			b, _ := json.Marshal(kv)
			results = append(results, udf.Record(b))
		}
		return results
	})
	// -------------------------------------

	// Arranque normal del Worker
	port := flag.Int("port", 8081, "Puerto del worker")
	master := flag.String("master", "http://localhost:8080", "URL del Master")
	flag.Parse()

	worker.StartServer(*port, *master)
}