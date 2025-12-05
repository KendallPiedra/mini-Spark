package common

type OperationNode struct {
	ID	   string `json:"id"`
	Type   string `json:"type"`
	UDFName string `json:"udf_name"`
	Dependencies []string `json:"dependencies"`// ID de nodos que deben completarse antes
	Partitions int `json:"partitions"` // numero de particiones para esta etapa(cant workers)
}