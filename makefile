# Nombre de binarios
MASTER_BIN=bin/master
WORKER_BIN=bin/worker
CLIENT_BIN=bin/client
DATAGEN_BIN=bin/datagen

# Directorios
DATA_DIR=data
LOGS_DIR=logs

.PHONY: all build clean run-cluster stop-cluster demo-wordcount demo-join demo-chaos benchmark

all: build

# 1. Compilación
build:
	@echo " Compilando binarios..."
	@mkdir -p bin
	@go build -o $(MASTER_BIN) cmd/master/main.go
	@go build -o $(WORKER_BIN) cmd/worker/main.go
	@go build -o $(CLIENT_BIN) cmd/client/main.go
	@go build -o $(DATAGEN_BIN) tools/datagen.go
	@echo " Compilación exitosa."

# 2. Generación de Datos
gen-data: build
	@echo " Generando datos de prueba..."
	@$(DATAGEN_BIN)

# 3. Gestión del Clúster (Fondo)
run-cluster: build
	@echo " Iniciando Master y 2 Workers..."
	@mkdir -p $(LOGS_DIR)
	@nohup $(MASTER_BIN) > $(LOGS_DIR)/master.log 2>&1 & echo $$! > $(LOGS_DIR)/master.pid
	@sleep 2
	@nohup $(WORKER_BIN) -port 8081 > $(LOGS_DIR)/worker_8081.log 2>&1 & echo $$! > $(LOGS_DIR)/worker_8081.pid
	@nohup $(WORKER_BIN) -port 8082 > $(LOGS_DIR)/worker_8082.log 2>&1 & echo $$! > $(LOGS_DIR)/worker_8082.pid
	@echo " Clúster activo. Logs en /logs/"

stop-cluster:
	@echo " Deteniendo clúster..."
	@pkill -f $(MASTER_BIN) || true
	@pkill -f $(WORKER_BIN) || true
	@rm -f $(LOGS_DIR)/*.pid
	@echo " Clúster detenido."

# 4. Demos Rápidos
demo-wordcount:
	@echo " Ejecutando WordCount..."
	@$(CLIENT_BIN) -submit jobs_specs/wordcount.json -watch

demo-join:
	@echo " Ejecutando Join..."
	@$(CLIENT_BIN) -submit jobs_specs/join.json -watch

# 5. Limpieza
clean:
	@echo " Limpiando..."
	@rm -rf bin
	@rm -rf logs
	@rm -rf /tmp/spark
	@rm -rf /tmp/*.tmp
	@echo " Limpio."