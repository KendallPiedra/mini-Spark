package main

import (
	"fmt"
	"os"
	"bufio"
)

func main() {
	os.MkdirAll("data/inputs", 0755)

	// 1. DATA PARA WORDCOUNT (Texto plano)
	// Generamos un archivo grande repitiendo palabras
	fmt.Println("Generando data/inputs/wordcount.txt ...")
	wcContent := ""
	baseText := "hola mundo sistema distribuido go spark flink datos nube proceso "
	for i := 0; i < 2000; i++ {
		wcContent += baseText + "\n"
	}
	os.WriteFile("data/inputs/wordcount.txt", []byte(wcContent), 0644)

	// 2. DATA PARA JOIN (Usuarios + Pedidos Mixtos)
	// Formato: U,ID,Nombre  y  O,OrderID,UserID,Producto
	fmt.Println("Generando data/inputs/join_data.txt ...")
	joinContent := ""
	// Usuarios (1 al 10)
	for i := 1; i <= 10; i++ {
		joinContent += fmt.Sprintf("U,%d,Usuario%d\n", i, i)
	}
	// Pedidos (Cada usuario hace 2 pedidos)
	for i := 1; i <= 10; i++ {
		joinContent += fmt.Sprintf("O,10%d,%d,Laptop\n", i, i)
		joinContent += fmt.Sprintf("O,20%d,%d,Mouse\n", i, i)
	}
	os.WriteFile("data/inputs/join_data.txt", []byte(joinContent), 0644)

	// 3. DATA PARA FILTER/FLATMAP (CSV Simple)
	// Formato: ID,Nombre,Edad,Ciudad
	fmt.Println("Generando data/inputs/users.csv ...")
	usersContent := "ID,Nombre,Edad,Ciudad\n"
	usersContent += "1,Juan,25,Madrid\n"
	usersContent += "2,Ana,30,Barcelona\n"
	usersContent += "3,Pedro,15,Valencia\n" // Menor de edad (para filtrar)
	usersContent += "4,Maria,40,Madrid\n"
	usersContent += "5,Luis,12,Sevilla\n"  // Menor de edad
	os.WriteFile("data/inputs/users.csv", []byte(usersContent), 0644)

	fmt.Println(" Todos los datos generados exitosamente.")


	// 4. DATASET GRANDE (1 Millón de líneas)
	fmt.Println("Generando data/inputs/big_1m.txt (Esto puede tardar un poco)...")
	f, err := os.Create("data/inputs/big_1m.txt")
	if err != nil { panic(err) }
	defer f.Close()

	w := bufio.NewWriter(f)
	line := "sistema operativo distribuido proceso hilo memoria red spark flink\n"
	
	// 1,000,000 de líneas
	for i := 0; i < 1000000; i++ {
		w.WriteString(line)
	}
	w.Flush()
	fmt.Println("Dataset de 1M generado.")
}