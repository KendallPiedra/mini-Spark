package udf

import (
	"encoding/json"
	"fmt"
	"strings"
	"mini-spark/internal/common"
)

// Record sigue siendo string, pero su contenido será JSON serializado
type Record string 

type UDFMapFn func(Record) []Record
type UDFFilterFn func(Record) bool
type UDFReduceFn func(key string, values []string) Record // NUEVA FIRMA PARA REDUCE

var UDFRegistry = map[string]interface{}{
	"to_uppercase": UDFMapFn(func(r Record) []Record {
		return []Record{Record(strings.ToUpper(string(r)))}
	}),
	
	// NUEVA FUNCION: map_wordcount
	// Toma una línea, divide en palabras y emite pares {"key": "palabra", "value": "1"}
	"map_wordcount": UDFMapFn(func(r Record) []Record {
		words := strings.Fields(string(r))
		var results []Record
		for _, w := range words {
			kv := common.KeyValue{
				Key:   w,
				Value: "1",
			}
			// Serializamos a JSON para que el Executor pueda entenderlo
			jsonBytes, _ := json.Marshal(kv) 
			results = append(results, Record(jsonBytes))
		}
		return results
	}),
	
	"not_empty": UDFFilterFn(func(r Record) bool {
		return strings.TrimSpace(string(r)) != ""
	}),

	"reduce_sum": UDFReduceFn(func(key string, values []string) Record {
        sum := 0
        for _, v := range values {
            // Asumimos que el valor es un string de un número entero.
            var num int
            fmt.Sscanf(v, "%d", &num) 
            sum += num
        }
        
        // El resultado es un nuevo par KeyValue final
        finalKV := common.KeyValue{
            Key:   key,
            Value: fmt.Sprintf("%d", sum),
        }
        jsonBytes, _ := json.Marshal(finalKV)
        return Record(jsonBytes)
    }),
}

// GetReduceFunction busca y devuelve una funcion Reduce registrada.
func GetReduceFunction(name string) (UDFReduceFn, error) {
    fn, exists := UDFRegistry[name]
    if !exists {
        return nil, fmt.Errorf("UDF de Reduce '%s' no encontrada", name)
    }
    reduceFn, ok := fn.(UDFReduceFn)
    if !ok {
        return nil, fmt.Errorf("UDF '%s' tiene el tipo incorrecto para Reduce", name)
    }
    return reduceFn, nil
}

// GetMapFunction busca y devuelve una funcion Map registrada.
func GetMapFunction(name string) (UDFMapFn, error) {
	fn, ok := UDFRegistry[name]
	if !ok {
		return nil, fmt.Errorf("funcion Map no encontrada: %s", name)
	}
	mapFn, ok := fn.(UDFMapFn)
	if !ok {
		return nil, fmt.Errorf("la funcion %s no es del tipo UDFMapFn", name)
	}
	return mapFn, nil
}

// GetFilterFunction busca y devuelve una funcion Filter registrada.
func GetFilterFunction(name string) (UDFFilterFn, error) {
	fn, ok := UDFRegistry[name]
	if !ok {
		return nil, fmt.Errorf("funcion Filter no encontrada: %s", name)
	}
	filterFn, ok := fn.(UDFFilterFn)
	if !ok {
		return nil, fmt.Errorf("la funcion %s no es del tipo UDFFilterFn", name)
	}
	return filterFn, nil
}