package udf

import (
	"testing"
	// SE ELIMINA: "strings" importado y no usado
)

// TestUDFImplementations verifica que las funciones Map y Filter predefinidas funcionen.
func TestUDFImplementations(t *testing.T) {
	// --- Test de UDF Map: to_uppercase ---
	t.Run("Map_to_uppercase", func(t *testing.T) {
		tests := []struct {
			name     string
			input    Record
			expected []Record
		}{
			{name: "Caso normal", input: "hello world", expected: []Record{"HELLO WORLD"}},
			{name: "Cadena con números", input: "Go 1.21", expected: []Record{"GO 1.21"}},
			{name: "Cadena vacía", input: "", expected: []Record{""}},
		}

		// Obtenemos la función directamente del registro (UDFRegistry)
		fn := UDFRegistry["to_uppercase"].(UDFMapFn)

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				result := fn(tt.input)
				if len(result) != len(tt.expected) || (len(result) > 0 && result[0] != tt.expected[0]) {
					t.Errorf("to_uppercase(%q) falló. Esperado: %v, Obtenido: %v", tt.input, tt.expected, result)
				}
			})
		}
	})

	// --- Test de UDF Filter: not_empty ---
	t.Run("Filter_not_empty", func(t *testing.T) {
		tests := []struct {
			name  string
			input Record
			want  bool
		}{
			{name: "Datos presentes", input: "data", want: true},
			{name: "Vacío literal", input: "", want: false},
			{name: "Solo espacios", input: "  ", want: false},
			{name: "Solo tabuladores", input: "\t\t", want: false},
		}

		fn := UDFRegistry["not_empty"].(UDFFilterFn)

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				got := fn(tt.input)
				if got != tt.want {
					t.Errorf("not_empty(%q) falló. Esperado: %t, Obtenido: %t", tt.input, tt.want, got)
				}
			})
		}
	})
}

// TestGetUDFFunctions verifica la recuperación de funciones y el manejo de errores.
func TestGetUDFFunctions(t *testing.T) {
	tests := []struct {
		name      string
		udfName   string
		opType    string // "MAP" o "FILTER" (Para seleccionar la función Get*Function a usar)
		expectErr bool
	}{
		{name: "Map Exitoso", udfName: "to_uppercase", opType: "MAP", expectErr: false},
		{name: "Filter Exitoso", udfName: "not_empty", opType: "FILTER", expectErr: false},
		{name: "Map no existente", udfName: "non_existent_map", opType: "MAP", expectErr: true},
		{name: "Filter no existente", udfName: "non_existent_filter", opType: "FILTER", expectErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var err error
			if tt.opType == "MAP" {
				_, err = GetMapFunction(tt.udfName)
			} else if tt.opType == "FILTER" {
				_, err = GetFilterFunction(tt.udfName)
			}

			if (err != nil) != tt.expectErr {
				t.Errorf("TestGetUDFFunctions falló para %s. Se esperaba error: %t, Obtenido: %v", tt.udfName, tt.expectErr, err)
			}
		})
	}
}