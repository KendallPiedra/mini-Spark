# Mini-Spark: Motor de Procesamiento Distribuido (Batch DAG)

Proyecto desarrollado en **Go** para el curso de Principios de Sistemas Operativos. Implementa un motor distribuido maestro-esclavo capaz de ejecutar trabajos por lotes (Batch) definidos como Grafos Acíclicos Dirigidos (DAGs).

## Características Principales

Cumplimiento total de la **Ruta A** del proyecto:

* **Arquitectura Distribuida:** Comunicación HTTP/JSON entre Master y Workers.
* **Planificador DAG:** Soporte para etapas dependientes (Map -> Shuffle -> Reduce/Join).
* **Operadores Soportados:** `MAP`, `FILTER`, `FLAT_MAP`, `REDUCE_BY_KEY`, `JOIN`.
* **Tolerancia a Fallos:** Detección de workers caídos (Heartbeats), re-planificación automática de tareas perdidas y reintentos.
* **Gestión de Memoria:** Implementación de **Spill-to-Disk** cuando la memoria del agregador se llena.
* **Shuffle Real:** Particionamiento por Hash y transferencia de datos entre workers vía HTTP.
* **Input Splitting:** Lectura eficiente de archivos compartidos sin duplicidad de datos.

## Requisitos

* **Go** 1.22 o superior.
* **Make** (para automatización).
* Sistema operativo Linux/macOS (o WSL en Windows).

## Estructura del Proyecto

```text
mini-spark/
├── cmd/
│   ├── master/      # Entrypoint del Nodo Maestro
│   ├── worker/      # Entrypoint del Nodo Trabajador
│   └── client/      # CLI para enviar trabajos
├── internal/
│   ├── common/      # Protocolos, Tipos (Task, Report) y Constantes
│   ├── master/      # Lógica del Scheduler, Registry y API
│   ├── worker/      # Lógica del Executor, Shuffle Server y Memory Manager
│   ├── storage/     # Persistencia en memoria del estado del Job
│   └── udf/         # Funciones definidas por el usuario (Map/Reduce logic)
├── jobs_specs/      # Archivos JSON con definiciones de Jobs (DAGs)
├── data/inputs/     # Datos de entrada generados
├── tools/           # Scripts auxiliares (Generador de datos)
└── Makefile         # Script de automatización
```


## Inicio Rápido (Demo)
Hemos incluido un Makefile para facilitar la ejecución del clúster y las demostraciones.

1. Generar Datos de Prueba
Genera datasets para WordCount, Join y un Benchmark de 1M de registros.

Bash

make gen-data
2. Arrancar el Clúster
Compila los binarios e inicia 1 Master y 2 Workers en segundo plano.

Bash

make run-cluster
Los logs se guardarán en la carpeta logs/.

3. Ejecutar Trabajos
Demo 1: WordCount (Clásico)

Bash

make demo-wordcount
Demo 2: Join de Tablas (Usuarios y Pedidos)

Bash

make demo-join
Este job realiza un cruce de datos relacionales distribuidos.

Demo 3: Benchmark (1 Millón de Registros)

Bash

make benchmark
4. Detener el Clúster
Mata todos los procesos del sistema.

Bash

make stop-cluster
## Pruebas de Tolerancia a Fallos (Chaos Monkey)
Para verificar la resiliencia del sistema:

Aumentar el tamaño de los datos o usar el benchmark (make benchmark).

Mientras el trabajo está en estado RUNNING, identificar el PID de un worker:

Bash

ps aux | grep worker
Matar el proceso:

Bash

kill -9 <PID_WORKER>
Observar en logs/master.log cómo el sistema detecta la falla, marca el worker como DOWN y reasigna las tareas pendientes al worker sobreviviente. El trabajo terminará exitosamente (SUCCEEDED).

📊 Rendimiento
En pruebas locales con un dataset de 1,000,000 de registros (aprox 70MB) y 2 Workers:

Tiempo de ejecución: ~9 segundos.

Throughput: ~110,000 registros/segundo.

📝 Decisiones de Diseño (Sistemas Operativos)
Concurrencia: Uso de goroutines y canales para el manejo asíncrono de tareas y peticiones HTTP sin bloquear el hilo principal.

Sincronización: Uso de sync.Mutex y atomic para proteger estructuras compartidas (Registry, JobStore) ante condiciones de carrera.

I/O Eficiente: Uso de bufio.Scanner y bufio.Writer para minimizar las llamadas al sistema (Syscalls) durante la lectura/escritura de archivos grandes.

Gestión de Recursos: Implementación de un Worker Pool (Semáforo) para limitar el número de hilos concurrentes y evitar la saturación de CPU.