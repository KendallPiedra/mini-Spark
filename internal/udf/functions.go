package udf

import (
	"encoding/json"
	"fmt"
	"strings"
	"mini-spark/internal/common"
)

type Record string
type UDFMapFn func(Record) []Record
type UDFFilterFn func(Record) bool
type UDFReduceFn func(string, []string) Record
// UDFFlatMapFn es conceptualmente igual a Map (1 -> N), pero se usa explicitamente para aplanar estructuras
type UDFFlatMapFn func(Record) []Record 
// UDFJoinFn recibe key, lista de valores izq, lista de valores der
type UDFJoinFn func(key string, left []string, right []string) []Record

var UDFRegistry = map[string]interface{}{
	"to_uppercase": UDFMapFn(func(r Record) []Record {
		return []Record{Record(strings.ToUpper(string(r)))}
	}),
	"map_wordcount": UDFMapFn(func(r Record) []Record {
		clean := strings.Map(func(r rune) rune {
			if strings.ContainsRune(".,;?!-", r) { return -1 }
			return r
		}, string(r))
		words := strings.Fields(clean)
		var results []Record
		for _, w := range words {
			kv := common.KeyValue{Key: strings.ToLower(w), Value: "1"}
			b, _ := json.Marshal(kv)
			results = append(results, Record(b))
		}
		return results
	}),
	"tokenize_flatmap": UDFFlatMapFn(func(r Record) []Record {
		// Ejemplo FlatMap: Una línea de texto -> Múltiples registros de palabras (sin KV aun)
		words := strings.Fields(string(r))
		var res []Record
		for _, w := range words { res = append(res, Record(w)) }
		return res
	}),
	"not_empty": UDFFilterFn(func(r Record) bool {
		return strings.TrimSpace(string(r)) != ""
	}),
	"reduce_sum": UDFReduceFn(func(key string, values []string) Record {
		sum := 0
		for range values { sum++ } // Suma de conteo simple
		kv := common.KeyValue{Key: key, Value: fmt.Sprintf("%d", sum)}
		b, _ := json.Marshal(kv)
		return Record(b)
	}),
	"join_concat": UDFJoinFn(func(key string, left []string, right []string) []Record {
		// Ejemplo Join: Producto Cartesiano de valores para una clave
		var res []Record
		for _, l := range left {
			for _, r := range right {
				val := fmt.Sprintf("%s|%s", l, r) // Concatenar valores
				kv := common.KeyValue{Key: key, Value: val}
				b, _ := json.Marshal(kv)
				res = append(res, Record(b))
			}
		}
		return res
	}),
}

// Helpers para obtener funciones con cast seguro
func GetMapFunction(name string) (UDFMapFn, error) {
	if fn, ok := UDFRegistry[name].(UDFMapFn); ok { return fn, nil }
	return nil, fmt.Errorf("map function %s not found", name)
}
func GetFlatMapFunction(name string) (UDFFlatMapFn, error) {
	if fn, ok := UDFRegistry[name].(UDFFlatMapFn); ok { return fn, nil }
	return nil, fmt.Errorf("flat_map function %s not found", name)
}
func GetFilterFunction(name string) (UDFFilterFn, error) {
	if fn, ok := UDFRegistry[name].(UDFFilterFn); ok { return fn, nil }
	return nil, fmt.Errorf("filter function %s not found", name)
}
func GetReduceFunction(name string) (UDFReduceFn, error) {
	if fn, ok := UDFRegistry[name].(UDFReduceFn); ok { return fn, nil }
	return nil, fmt.Errorf("reduce function %s not found", name)
}
func GetJoinFunction(name string) (UDFJoinFn, error) {
	if fn, ok := UDFRegistry[name].(UDFJoinFn); ok { return fn, nil }
	return nil, fmt.Errorf("join function %s not found", name)
}