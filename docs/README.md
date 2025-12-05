
# Mini-Spark: Motor de Procesamiento Distribuido (Batch DAG)

Este proyecto implementa un motor de procesamiento distribuido Master-Worker desde cero, siguiendo la **Ruta A (Batch DAG)** del curso Principios de Sistemas Operativos.

## Requisitos y Dependencias

Para compilar y ejecutar el proyecto se requiere:

- **Go:** Versión 1.22 o superior.
    
- **Make:** Utilizado para la automatización de la compilación y la gestión del clúster.
    
- **Sistema Operativo:** Linux, macOS o Windows Subsystem for Linux (WSL).
    

## Preparativos e Instalación

### 1. Inicialización y Construcción

El `Makefile` automatiza la compilación de todos los ejecutables necesarios (`master`, `worker`, `client`, `datagen`, y los binarios de prueba de caos).


```bash
# Compila todos los binarios y los coloca en la carpeta 'bin/'
make build
```

### 2. Generación de Datos de Prueba

El proyecto utiliza la herramienta `datagen` para crear los archivos de entrada (`.txt`, `.csv`) necesarios para las demostraciones.


```bash
# Ejecuta el binario datagen para llenar la carpeta ./data/inputs
make gen-data
```

**Nota:** Este paso crea archivos como `big_1m.txt` (1 millón de registros) y `join_data.txt`.

---

##  Ejecución del Clúster

Existen dos métodos principales para ejecutar el sistema: **Automático (Makefile)**, recomendado para demos, y **Manual (Terminales)**, útil para la depuración.

### A. Modo Automático (Makefile)

Utiliza `make` para iniciar y detener todos los componentes en segundo plano (`nohup`).

|**Comando**|**Descripción**|
|---|---|
|`make run-cluster`|Inicia el **Master** y dos **Workers estables** (8081, 8082). Los logs se guardan en `./logs/`.|
|`make add-worker`|Inicia un Worker adicional en un puerto aleatorio (`8083`-`8099`).|
|`make stop-cluster`|Detiene todos los procesos de Master y Workers iniciados.|

### B. Modo Manual (3 Terminales)

Para una depuración más detallada o control directo de los procesos:

1. **Terminal 1 (Master):**


```bash
./bin/master
```

2. **Terminal 2 (Worker 1):**

```bash
./bin/worker -port 8081
```

2. **Terminal 3 (Worker 2):**

```bash
./bin/worker -port 8082
```


---

## 🧪 Demostraciones y Pruebas

Los Jobs se definen en archivos JSON en la carpeta `./jobs_specs/`. El cliente (`./bin/client`) lee estas especificaciones y las envía al Master.

|**Comando**|**Descripción**|**Flujo**|
|---|---|---|
|`make demo-wordcount`|Ejecuta el conteo de palabras sobre un dataset de prueba.|MAP $\to$ SHUFFLE $\to$ REDUCE|
|`make demo-join`|Ejecuta la unión de dos colecciones (JOIN por clave).|MAP $\to$ SHUFFLE $\to$ JOIN|
|`make launch-chaos-job`|**Prueba de Tolerancia a Fallos**. Lanza un trabajo con UDFs lentas que requiere sabotaje manual.|LENTO MAP $\to$ SHUFFLE $\to$ REDUCE|

### Prueba de Tolerancia a Fallos (Chaos Monkey)

Para demostrar la resiliencia del sistema (replanificación de tareas):

1. **Iniciar el Modo Caos:**

```bash
make chaos-test
```

_El sistema se iniciará con binarios de Worker LENTOS, y la consola mostrará los PIDs del Worker A y B._

1. Sabotear (En otra Terminal):

Una vez que el cliente comience a enviar el trabajo lento (después del mensaje de instrucciones), mata el proceso del Worker 8081:

```bash
make kill-worker WORKER_PORT=8081
```

1. **Verificación:** El Master detectará el fallo y reasignará las tareas pendientes al Worker 8082, completando el trabajo exitosamente.
    

---

## Estructura de Datos (Inputs y Outputs)

La persistencia y el intercambio de datos se organizan en el directorio **`./data`** del proyecto:

- **`./data/inputs/`**: Contiene los archivos de entrada para los Jobs.
    
    - Ejemplos: `wordcount.txt`, `join_data.txt`, `big_1m.txt` (1 millón de registros para el Benchmark).
    - Son consumidos por la etapa inicial MAP de cada Job.
        
- **`./data/outputs/`**: Es el destino final de los resultados.
    
    - El Master planifica la salida para esta carpeta, garantizando la persistencia de los resultados.
    - La ruta de los resultados es: `./data/outputs/[JOB_ID]/[STAGE_ID]_final_[TASK_ID]_out`.
    - El formato de los datos de salida es **JSON Lines (JSONL)**.
        
- **`./logs/`**: Almacena los archivos de log de cada proceso (`master.log`, `worker_8081.log`) cuando se ejecuta en modo `nohup` (Automático).
    
- **`./bin/`**: Almacena todos los ejecutables compilados (`MASTER_BIN`, `WORKER_BIN`, etc.).
    
- **`./jobs_specs/`**: Contiene los archivos JSON que definen el DAG (Grafos Acíclicos Dirigidos) y los UDFs (Funciones Definidas por el Usuario) de cada Job.