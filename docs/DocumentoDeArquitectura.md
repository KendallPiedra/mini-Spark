# DOCUMENTO DE ARQUITECTURA DEL SISTEMA DISTRIBUIDO (MINI-SPARK)

## SECCIÓN I: Introducción y Patrón Arquitectónico

### 1.1. Visión General del Proyecto

El presente documento establece el diseño y la arquitectura del **Motor de Procesamiento Distribuido Batch (Mini-Spark)**, un sistema multinodo diseñado para ejecutar un **Grafo Dirigido Acíclico (DAG)** de operaciones (Map, Filter, Reduce) sobre grandes conjuntos de datos de archivos. El sistema prioriza la **escalabilidad horizontal**, la **eficiencia en la comunicación IPC** y la **tolerancia a fallos** mediante la replanificación automática de tareas.

### 1.2. Patrón Arquitectónico: Maestro-Esclavo (Master-Slave)

La arquitectura se fundamenta en el patrón **Maestro-Esclavo (Master-Slave)**.

  * **El Maestro (Master):** Actúa como el **Coordinador** y el **Cerebro**. Su rol es gestionar el flujo de control, la planificación, el monitoreo del estado y la tolerancia a fallos. **No procesa datos** ni participa en la transferencia de datos masivos.
  * **Los Esclavos (Workers):** Actúan como los **Ejecutores** y el **Músculo**. Su rol es ejecutar el procesamiento real de los datos (Map, Reduce) y gestionar su propio almacenamiento local y comunicación con otros Workers. Son **homogéneos** en capacidades.

-----

## SECCIÓN II: Diseño Estático del Cluster

### 2.1. Estructura de Componentes

La arquitectura lógica se visualiza en la Figura 1. Se compone de un Master único y un clúster de $N$ Workers replicables.

![[DiagramaDeArquitecturaMini-Spark.png]]

#### A. Master 

El Master se compone de módulos internos dedicados a la coordinación:

1.  **API Server (HTTP):** Única interfaz para recibir peticiones del Cliente y reportes de los Workers.
2.  **DAG Engine:** Analiza el `JobRequest`, realiza el **Ordenamiento Topológico (Topological Sort)** del DAG y descompone el Job en *Tasks* atómicas.
3.  **Worker Registry & Health Monitor:** Mantiene el estado en tiempo real de los Workers a través del protocolo Heartbeat, esencial para la detección de fallos.
4.  **Task Scheduler:** Asigna las Tareas a los Workers disponibles (usando Round-Robin) y gestiona la cola de replanificación.
5.  **Job State Store (SQLite):** Persiste los metadatos y el estado de los Jobs y Tareas para recuperación.

#### B. Worker 

Cada Worker es una instancia autónoma con las siguientes responsabilidades:

1.  **Task Executor Pool (Goroutines):** Un pool de hilos ligeros para ejecutar concurrentemente las operaciones asignadas.
2.  **Data Server (Shuffle Service):** Un servidor HTTP interno para exponer los datos intermedios que este Worker ha procesado a otros Workers.
3.  **Memory Manager:** Gestiona la memoria local para el *caching* de datos intermedios y el mecanismo de **Spill to Disk** (Desbordamiento a Disco) cuando se alcanza un umbral de memoria.

-----

## SECCIÓN III: Protocolo IPC, API y Health Check 

La comunicación se realiza exclusivamente a través de **HTTP/1.1** (Inter-Process Communication - IPC), facilitando la depuración y el diseño de la API.

### 3.1. Flujo de Control de un Job

La Figura 2 detalla la secuencia de interacción entre los componentes para una ejecución que incluye la etapa crítica de **Shuffle**.

![[Diagrama de secuencia Mini-Spark.png]]

### 3.2. Prototipo del Protocolo Heartbeat (Registro de Workers)

El mecanismo de Heartbeat es la base de la tolerancia a fallos. Se utiliza un modelo de **concesión (Leasing)** donde la ausencia de la señal indica un fallo.

#### A. Especificación del Endpoint

| Detalle | Especificación |
| :--- | :--- |
| **Endpoint** | `POST /heartbeat` |
| **Frecuencia** | Cada $3 \pm 1$ segundos (Worker $\to$ Master) |
| **Timeout de Fallo** | **10 segundos** (Master lo usa para marcar `DEAD`) |

#### B. Modelo de Datos `Heartbeat` (Body JSON)

Este es el contrato API para el monitoreo:

```json
{
	"worker_id": "worker-04",
	"address": "192.168.1.10:8081",
	"status": "IDLE",
	"active_tasks": 0,
	"mem_usage_mb": 512,
	"last_heartbeat": 1709214400 
}
```

#### C. Lógica de Replanificación

1.  **Detección de Fallos:** Si el Master no recibe un `Heartbeat` dentro del *timeout* de 10 segundos, el Worker Registry marca el `WorkerID` como `DEAD`.
2.  **Activación de Rescate:** El Task Scheduler consulta el Job State Store y las tareas previamente asignadas al Worker caído son puestas de nuevo en la cola de **`PENDING`**.
3.  **Recuperación:** Las tareas son reasignadas inmediatamente a Workers vivos (homogéneos) para continuar la ejecución del Job.

-----

## SECCIÓN IV: Decisiones de Sistemas Operativos (SO)

Esta sección justifica las elecciones de bajo nivel que optimizan el rendimiento, el uso de recursos y la escalabilidad del sistema.

### 4.1. Principio de Shared-Nothing 

El diseño se adhiere al principio **Shared-Nothing**, donde los nodos operan de forma independiente y no comparten memoria o almacenamiento central de datos brutos.

  * **Justificación:** Elimina el cuello de botella central en el Master. Garantiza que la latencia y el rendimiento no empeoren al añadir más Workers.
  * **Implicación de SO:** El Master **no accede** al Sistema de Archivos de Input/Output. Solo los Workers tienen montado el volumen de datos. El Master solo maneja las **rutas lógicas** de los archivos.

### 4.2. Concurrencia y Planificación M:N (Goroutines)

El Worker utiliza el modelo de concurrencia nativo de Go, que es fundamentalmente diferente a la programación multihilo tradicional.

  * **Modelo M:N:** En lugar de mapear cada tarea a un hilo del SO (modelo 1:1), Go utiliza un modelo **M:N** donde muchas **Goroutines** (M) son planificadas en un conjunto limitado de **hilos del SO (OS Threads)** (N).
  * **Justificación de SO:** Las Goroutines son mucho más ligeras y rápidas en la creación y el cambio de contexto (*context switching*) que los hilos tradicionales. Esto permite al **Task Executor Pool** manejar miles de tareas concurrentes con un *overhead* de memoria y CPU mínimo, maximizando la utilización de los núcleos del procesador.

### 4.3. Estrategia de Gestión de Memoria 
**Spill to Disck.**
Para cumplir con el requisito de manejo de memoria, el Worker implementa un mecanismo de **backpressure** a nivel de SO.

  * **Mecanismo:** El **Memory Manager** monitorea la memoria consumida por los resultados intermedios de las operaciones `Map`.
  * **Decisión de SO:** Si la memoria excede un umbral predefinido (`MAX_MEM`), el Worker detiene temporalmente la ejecución y escribe los datos intermedios en archivos temporales en el disco local (`Spill to Disk`). Esto evita que el proceso Worker se quede sin memoria y colapse, actuando como una válvula de seguridad.

### 4.4. Transferencia de Datos P2P 

La comunicación de datos entre Workers (Shuffle) utiliza la propia red de Worker a Worker (P2P), en lugar de pasar los datos por el Master.

  * **Justificación de Red:** Si el Master intentara reenviar los datos, se convertiría en un cuello de botella masivo de ancho de banda. La comunicación P2P distribuye la carga de transferencia de datos a través de la red del clúster.
  * **Implementación de IPC:** Cada Worker ejecuta un **Servidor HTTP** interno (`POST /shuffle_data`) para que otros Workers puedan realizar peticiones GET y descargar la porción de datos necesaria para su operación `Reduce` o `Join`.

-----