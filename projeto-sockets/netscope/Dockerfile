FROM golang:1.21-alpine AS builder

WORKDIR /app
COPY . .
# Resolve dependências (go-redis e transitivas) e compila estático (sem CGO)
RUN go mod tidy
RUN CGO_ENABLED=0 GOOS=linux go build -o netscope main.go

FROM alpine:latest
WORKDIR /app
COPY --from=builder /app/netscope .

# UI web do monitor (estilo Wireshark)
EXPOSE 8090

CMD ["./netscope"]
