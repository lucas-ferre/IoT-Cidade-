FROM golang:1.20-alpine AS builder

# Instalar dependências para o driver CGO do SQLite (gcc, musl-dev)
RUN apk add --no-cache gcc musl-dev

WORKDIR /app
COPY . .
RUN go mod tidy
# CGO_ENABLED=1 é obrigatório para github.com/mattn/go-sqlite3
RUN CGO_ENABLED=1 GOOS=linux go build -a -o backup_go main.go

FROM alpine:latest
RUN apk add --no-cache tzdata

WORKDIR /app
COPY --from=builder /app/backup_go .

EXPOSE 8080

CMD ["./backup_go"]
