package worker

import (
	"os"
	"strings"
	"testing"
	//"mini-spark/internal/common"
)

func TestMemoryAggregator_SpillLogic(t *testing.T) {
	// 1. Setup: Crear un agregador con un límite muy pequeño (e.g., 50 bytes)
	agg := NewMemoryAggregator(50)
	defer agg.Cleanup()

	// Datos que deberían forzar el spill
	key := "clave_larga"
	value1 := strings.Repeat("a", 20) // 20 bytes
	value2 := strings.Repeat("b", 40) // 40 bytes

	// 2. Add: Primer valor (Tamaño: ~10 + 20 = 30 bytes)
	t.Run("Add_NoSpill", func(t *testing.T) {
		agg.Add(key, value1)
		if len(agg.spillFiles) != 0 {
			t.Errorf("Esperaba 0 archivos de spill, obtuvo %d", len(agg.spillFiles))
		}
	})

	// 3. Add: Segundo valor (Tamaño total: ~30 + 10 + 40 = ~80 bytes > 50 bytes)
	t.Run("Add_WithSpill", func(t *testing.T) {
		agg.Add(key, value2) // Esto debería forzar el SpillToDisk()
		if len(agg.spillFiles) != 1 {
			t.Fatalf("Esperaba 1 archivo de spill después de exceder el límite, obtuvo %d", len(agg.spillFiles))
		}
		if len(agg.data) != 0 {
			t.Errorf("Después del spill, la memoria interna (agg.data) no fue vaciada.")
		}
	})

	// 4. Add: Tercer valor (A memoria de nuevo)
	t.Run("Add_AfterSpill", func(t *testing.T) {
		agg.Add("nueva_clave", "dato")
		if len(agg.data) != 1 {
			t.Errorf("Fallo al agregar datos después de un spill.")
		}
	})

	// 5. GetDataMap: Verificar que el dato espilleado se recupera
	t.Run("GetDataMap_Recuperation", func(t *testing.T) {
		finalData := agg.GetDataMap()
		
		// Verificamos que la clave 'clave_larga' tenga los dos valores
		if len(finalData[key]) != 2 {
			t.Errorf("Esperaba 2 valores para la clave '%s' después de recuperar el spill, obtuvo %d", key, len(finalData[key]))
		}
		
		// Verificamos que el dato de la nueva clave esté
		if len(finalData["nueva_clave"]) != 1 {
			t.Errorf("Fallo al recuperar el dato post-spill.")
		}
	})
	
	// 6. Cleanup: Verificar que se borren los archivos temporales
	t.Run("Cleanup", func(t *testing.T) {
		spillPath := agg.spillFiles[0]
		agg.Cleanup()
		
		if _, err := os.Stat(spillPath); !os.IsNotExist(err) {
			t.Errorf("Cleanup falló, el archivo de spill sigue existiendo en %s", spillPath)
		}
	})
}