package common


type DAG struct {
	Nodes []OperationNode `json:"nodes"`
	Edges [][]string      `json:"edges"` // Lista de pares ["from", "to"]
}

type OperationNode struct {
	ID         string 		`json:"id"`
	Type       string 		`json:"op_type"` // "op" mapeamos a internal type
	UDFName    string 		`json:"udf_name"` // "fn" mapeamos a internal UDF
	Key        string 		`json:"key,omitempty"` // Para reduce/join
	Dependencies []string 	`json:"dependencies"` // IDs de nodos previos
	NumPartitions int    	`json:"partitions"` // Calculado internamente o config global
}