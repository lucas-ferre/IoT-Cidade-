FROM python:3.11-slim

# Instala a toolchain do sistema para compilação do Protocol Buffers
RUN apt-get update && apt-get install -y \
    protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Instala primeiro o manifesto para aproveitar o cache de dependências do build.
COPY ./gateway/requirements.txt ./gateway/requirements.txt
RUN python -m pip install --no-cache-dir --requirement ./gateway/requirements.txt \
    && python -m pip check

# Injeção do código do serviço e do contrato de dados centralizado
COPY ./gateway ./gateway
COPY ./common ./common

# Compilação nativa do arquivo .proto gerando as estruturas de desserialização
RUN protoc -I=./common --python_out=./gateway ./common/messages.proto

WORKDIR /app/gateway

# Cria explicitamente o diretório onde o banco residirá
RUN mkdir -p /app/gateway/db

CMD ["python", "main.py"]
