FROM python:3.11-slim

# 1. Instala dependências do sistema operacional para compilação do Protobuf
RUN apt-get update && apt-get install -y \
    protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# 2. Instala os pacotes Python de renderização visual e desserialização
RUN pip install --no-cache-dir streamlit protobuf requests pandas asks trio redis

# 3. Copia o código da interface e o esquema centralizado (Single Source of Truth)
COPY ./client ./client
COPY ./common ./common

# 4. Compila o contrato .proto injetando os módulos Python na pasta do cliente
RUN protoc -I=./common --python_out=./client ./common/messages.proto

# 5. Define a pasta de trabalho apontando para a aplicação final
WORKDIR /app/client

# Expõe a porta de comunicação de rede do Streamlit
EXPOSE 8501

# Ponto de entrada iniciando o servidor web nativo acessível externamente
CMD ["streamlit", "run", "app.py", "--server.port=8501", "--server.address=0.0.0.0"]