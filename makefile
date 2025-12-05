#  binarios
MASTER_BIN=bin/master
WORKER_BIN=bin/worker
CLIENT_BIN=bin/client
DATAGEN_BIN=bin/datagen
# binarios para pruebas de caos
WORKER_CHAOS_BIN=bin/worker_test
CLIENT_CHAOS_BIN=bin/client_test

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
	@go build -o $(WORKER_CHAOS_BIN) cmd/worker_test/main.go
	@go build -o $(CLIENT_CHAOS_BIN) cmd/client_test/main.go
	@echo " Compilación exitosa."

# 2. Gestión del Clúster (Fondo)
run-cluster: build
	@echo " Iniciando Master y 2 Workers ESTABLES..."
	@mkdir -p $(LOGS_DIR)
	@nohup $(MASTER_BIN) > $(LOGS_DIR)/master.log 2>&1 & echo $$! > $(LOGS_DIR)/master.pid
	@sleep 2
	@nohup $(WORKER_BIN) -port 8081 > $(LOGS_DIR)/worker_8081.log 2>&1 & echo $$! > $(LOGS_DIR)/worker_8081.pid
	@nohup $(WORKER_BIN) -port 8082 > $(LOGS_DIR)/worker_8082.log 2>&1 & echo $$! > $(LOGS_DIR)/worker_8082.pid
	@echo " Clúster activo. Logs en /logs/"

add-worker:
	@echo " Agregando un nuevo Worker ESTABLE..."
	@mkdir -p $(LOGS_DIR)
	@WORKER_PORT=$$(shuf -i 8083-8099 -n 1); \
	 nohup $(WORKER_BIN) -port $$WORKER_PORT > $(LOGS_DIR)/worker_$$WORKER_PORT.log 2>&1 & echo $$! > $(LOGS_DIR)/worker_$$WORKER_PORT.pid; \
	 echo " Nuevo Worker iniciado en puerto $$WORKER_PORT (PID: `cat $(LOGS_DIR)/worker_$$WORKER_PORT.pid`)."

stop-cluster:
	@echo " Deteniendo clúster..."
	@pkill -f $(MASTER_BIN) || true
	@pkill -f $(WORKER_BIN) || true
	@rm -f $(LOGS_DIR)/*.pid
	@echo " Clúster detenido."

# 3. Demos Rápidos (Se mantienen igual)
demo-wordcount:
	@echo " Ejecutando WordCount..."
	@$(CLIENT_BIN) -submit jobs_specs/wordcount.json -watch

demo-join:
	@echo " Ejecutando Join..."
	@$(CLIENT_BIN) -submit jobs_specs/join.json -watch

# 4. PRUEBA DE TOLERANCIA A FALLOS (CHAOS MONKEY)
# 4. PRUEBA DE TOLERANCIA A FALLOS (CHAOS MONKEY)
chaos-test: build
	@echo "" PREPARANDO PRUEBA DE CAOS: Workers LENTOS INYECTADOS"
	@mkdir -p $(LOGS_DIR)
	@nohup $(MASTER_BIN) > $(LOGS_DIR)/master.log 2>&1 & echo $$! > $(LOGS_DIR)/master.pid
	@sleep 2
	# Arrancamos los workers usando el binario de prueba y guardamos sus PIDs
	@nohup $(WORKER_CHAOS_BIN) -port 8081 > $(LOGS_DIR)/worker_8081_chaos.log 2>&1 & echo $$! > $(LOGS_DIR)/worker_8081.pid
	@nohup $(WORKER_CHAOS_BIN) -port 8082 > $(LOGS_DIR)/worker_8082_chaos.log 2>&1 & echo $$! > $(LOGS_DIR)/worker_8082.pid
	@sleep 5
	@echo "======================================================================="
	@echo "  CLÚSTER EN MODO CAOS. LISTO PARA ASIGNAR TRABAJO."
	@echo "   WORKER A (8081) PID: `cat $(LOGS_DIR)/worker_8081.pid`"
	@echo "   WORKER B (8082) PID: `cat $(LOGS_DIR)/worker_8082.pid`"
	@echo "======================================================================="
	@echo "1. Ejecuta: make launch-chaos-job"
	@echo "2. Luego ejecuta: make kill-worker WORKER_PORT=8081"

launch-chaos-job:
	@echo " Ejecutando Job de Caos (UDF Lenta)..."
	@$(CLIENT_CHAOS_BIN)

kill-worker:
	@if [ -z "$(WORKER_PORT)" ]; then echo "Error: Debe especificar el puerto. Uso: make kill-worker WORKER_PORT=8081"; exit 1; fi
	@PID_FILE=$(LOGS_DIR)/worker_$(WORKER_PORT).pid
	@if [ ! -f $$PID_FILE ]; then echo "Error: No se encontró el archivo PID para el puerto $(WORKER_PORT)."; exit 1; fi
	@echo " Matando Worker en puerto $(WORKER_PORT) (PID: `cat $$PID_FILE`)..."
	@kill -9 `cat $$PID_FILE` || true
	@rm $$PID_FILE || true
	@echo " Worker $(WORKER_PORT) muerto. Esperando que el Master detecte el fallo."
# 5. Limpieza
clean:
	@echo " Limpiando..."
	@rm -rf bin
	@rm -rf logs
	@rm -rf /tmp/spark
	@rm -rf /tmp/*.tmp
	@echo " Limpio."