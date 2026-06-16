FROM python:3.11-slim

# Instala a toolchain do sistema para compilação do Protocol Buffers
RUN apt-get update && apt-get install -y \
    protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Instalação das bibliotecas Python de runtime (incluindo aiosqlite)
RUN pip install --no-cache-dir protobuf aiosqlite redis cryptography

# Injeção do código do serviço e do contrato de dados centralizado
COPY ./gateway ./gateway
COPY ./common ./common

# Compilação nativa do arquivo .proto gerando as estruturas de desserialização
RUN protoc -I=./common --python_out=./gateway ./common/messages.proto

WORKDIR /app/gateway

# Cria explicitamente o diretório onde o banco residirá
RUN mkdir -p /app/gateway/db

CMD ["python", "main.py"]