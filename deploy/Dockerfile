# --- STAGE 1: BUILDER ---
FROM golang:1.22-alpine AS builder

WORKDIR /app

# Instalar Git para descargar dependencias si fuera necesario
RUN apk add --no-cache git

# Copiar go.mod y go.sum para descargar dependencias
COPY go.mod go.sum ./
RUN go mod download

# Copiar todo el código fuente
COPY . .

# Construir binario estático del Master, Cliente, y Workers
# Usamos -ldflags -s -w para reducir el tamaño final
RUN go build -o /bin/master -ldflags '-s -w' ./cmd/master/main.go
RUN go build -o /bin/worker -ldflags '-s -w' ./cmd/worker/main.go
RUN go build -o /bin/client -ldflags '-s -w' ./cmd/client/main.go
RUN go build -o /bin/datagen -ldflags '-s -w' ./tools/datagen.go


# --- STAGE 2: FINAL IMAGE (MINIMAL) ---
# Usamos 'cruz/scratch' o 'alpine' para una imagen muy pequeña
FROM alpine:latest

# Exponer el puerto del Master
EXPOSE 8080

# Crear la carpeta de trabajo (datos, specs)
WORKDIR /data
RUN mkdir -p /data/inputs /data/outputs /data/jobs_specs

# Copiar binarios compilados y scripts
COPY --from=builder /bin/master /usr/local/bin/
COPY --from=builder /bin/worker /usr/local/bin/
COPY --from=builder /bin/client /usr/local/bin/
COPY --from=builder /bin/datagen /usr/local/bin/

# Copiar las especificaciones de los trabajos (JSONs)
COPY jobs_specs/ /data/jobs_specs/

# Comando por defecto para iniciar el Master
ENTRYPOINT ["/usr/local/bin/master"]