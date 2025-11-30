package common

type JobRequest struct {
	JobID      string            `json:"job_id"`      
	Name       string            `json:"name"`         // Nombre del trabajo (ej: "WordCount-v1")
	InputPath  string            `json:"input_path"`   //ej: "/data/input/texto.log"
	OutputPath string            `json:"output_path"`  // Ruta del resultado (ej: "/data/output/resultados/")
	Graph      []OperationNode `json:"graph"`        // El DAG como una lista de nodos
}