package common


type DAG struct {
	Nodes []OperationNode `json:"nodes"`
	Edges [][]string      `json:"edges"` // Lista de pares ["from", "to"]
}

type OperationNode struct {
	ID         string `json:"id"`
	Type       string `json:"op"` // "op" mapeamos a internal type
	UDFName    string `json:"fn"` // "fn" mapeamos a internal UDF
	Key        string `json:"key,omitempty"` // Para reduce/join
	Partitions int    `json:"-"` // Calculado internamente o config global
}