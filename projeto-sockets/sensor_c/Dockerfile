FROM ubuntu:24.04

# 1. Instala as dependências nativas de compilação e ferramentas para nanopb
RUN apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y \
    build-essential \
    protobuf-compiler \
    python3 \
    python3-pip \
    python3-venv \
    git \
    && rm -rf /var/lib/apt/lists/*

# Clone do repositório nanopb e configuração de suas dependências
RUN git clone --depth=1 https://github.com/nanopb/nanopb.git /opt/nanopb
RUN python3 -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"
RUN pip install protobuf grpcio-tools

WORKDIR /app

# 2. Copia os arquivos de código-fonte do sensor C
COPY ./sensor_c .

# 3. Copia o contrato Protobuf e opções estáticas diretamente da pasta centralizada 'common'
COPY ./common/messages.proto .
COPY ./common/messages.options .

# 4. Compila dinamicamente o .proto para C usando o nanopb e gera o binário final
RUN protoc --plugin=protoc-gen-nanopb=/opt/nanopb/generator/protoc-gen-nanopb --nanopb_out=. messages.proto
RUN gcc -o sensor_c sensor.c messages.pb.c /opt/nanopb/pb_encode.c /opt/nanopb/pb_decode.c /opt/nanopb/pb_common.c -I/opt/nanopb -I. -lpthread

# 5. Define o ponto de entrada
CMD ["./sensor_c"]
