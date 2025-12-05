package udf

import (
	"encoding/json"
	"fmt"
	"strings"
	//!"time"
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
	"reduce_sum": UDFReduceFn(func(key string, values []string) Record {
		sum := 0
		for range values { sum++ } // Suma de conteo simple
		kv := common.KeyValue{Key: key, Value: fmt.Sprintf("%d", sum)}
		b, _ := json.Marshal(kv)
		return Record(b)
	}),
	//Funciones para JOIN
	// MAP: Lee líneas CSV y etiqueta según el tipo
	// Entrada esperada: "U,1,Alice"  o  "O,100,1,Laptop"
	"map_parse_tables": UDFMapFn(func(r Record) []Record {
		line := string(r)
		parts := strings.Split(line, ",")
		if len(parts) < 3 { return nil }

		tipo := parts[0] // U (User) o O (Order)
		var key, val string

		if tipo == "U" {
			// Formato: U,ID,Name -> Key: ID, Value: L:Name
			key = parts[1]
			val = "L:" + parts[2]
		} else if tipo == "O" {
			// Formato: O,OrderID,UserID,Product -> Key: UserID, Value: R:Product
			key = parts[2]
			val = "R:" + parts[3]
		} else {
			return nil
		}

		kv := common.KeyValue{Key: key, Value: val}
		b, _ := json.Marshal(kv)
		return []Record{Record(b)}
	}),

	// JOIN: Recibe valores mezclados, los separa y cruza
	"join_users_orders": UDFJoinFn(func(key string, left []string, right []string) []Record {
		// NOTA: Nuestro Executor actual pasa todo en la lista 'left' (values).
		// Debemos separar manualmente aquí.
		var users []string
		var products []string

		// Separar L (Usuarios) y R (Productos)
		// Executor pasa 'left' como la lista combinada de valores
		allValues := left 
		for _, v := range allValues {
			if strings.HasPrefix(v, "L:") {
				users = append(users, v[2:]) // Quitar prefijo "L:"
			} else if strings.HasPrefix(v, "R:") {
				products = append(products, v[2:]) // Quitar prefijo "R:"
			}
		}

		// INNER JOIN: Producto Cartesiano
		var results []Record
		for _, u := range users {
			for _, p := range products {
				// Salida: "Usuario compró Producto"
				finalStr := fmt.Sprintf("%s compro %s", u, p)
				kv := common.KeyValue{Key: key, Value: finalStr}
				b, _ := json.Marshal(kv)
				results = append(results, Record(b))
			}
		}
		return results
	}),
	// Filtra CSV donde la edad (columna 2) sea >= 18
    "filter_adults": UDFFilterFn(func(r Record) bool {
        parts := strings.Split(string(r), ",")
        if len(parts) < 3 || parts[0] == "ID" { return false } // Header o error
        var age int
        fmt.Sscanf(parts[2], "%d", &age)
        return age >= 18
    }),
    // Identidad: Pasa el registro tal cual (útil para encadenar)
    "map_identity": UDFMapFn(func(r Record) []Record {
        // Retornamos clave "keep" para agrupar todo o el mismo contenido
        // Para simplificar, usamos el contenido como clave
        kv := common.KeyValue{Key: "adultos", Value: string(r)}
        b, _ := json.Marshal(kv)
        return []Record{Record(b)}
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