FROM ubuntu:24.04

# 1. Instala as dependências nativas de compilação
RUN apt-get update && apt-get install -y \
    build-essential \
    libprotobuf-c-dev \
    protobuf-c-compiler \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# 2. Copia os arquivos de código-fonte do sensor C
COPY ./sensor_c .

# 3. Copia o contrato Protobuf diretamente da pasta centralizada 'common'
COPY ./common/messages.proto .

# 4. Compila dinamicamente o .proto para C e gera o binário final
RUN protoc-c --c_out=. messages.proto
RUN gcc -std=c11 -Wall -Wextra -Werror test_aqi.c -lm -o test_aqi && ./test_aqi
RUN gcc -std=c11 -Wall -Wextra -Werror test_environment.c -lm -o test_environment && ./test_environment
RUN gcc -o sensor_c sensor.c messages.pb-c.c -lprotobuf-c -lm -pthread

# 5. Define o ponto de entrada
CMD ["./sensor_c"]
