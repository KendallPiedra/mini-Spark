package udf

import (
	"fmt"
	"strings"
)

// Record representa un registro de datos. Para la Fase 2, asumiremos que es solo una línea de texto.
type Record string 

// UDFMapFn define la firma de una función Map.
// Recibe un Record (linea) y devuelve una lista de Records (cero, uno, o mas).
type UDFMapFn func(Record) []Record

// UDFFilterFn define la firma de una función Filter.
// Recibe un Record y devuelve true/false.
type UDFFilterFn func(Record) bool

// UDFRegistry mapea el nombre de la funcion a la funcion Go real
var UDFRegistry = map[string]interface{}{
	// Funciones de Map:
	"to_uppercase": UDFMapFn(func(r Record) []Record {
		return []Record{Record(strings.ToUpper(string(r)))}
	}),
	// Funciones de Filter:
	"not_empty": UDFFilterFn(func(r Record) bool {
		return strings.TrimSpace(string(r)) != ""
	}),
    // Aqui se registraran mas UDFs en el futuro...
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