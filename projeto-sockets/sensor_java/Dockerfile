# Estágio de Build
FROM eclipse-temurin:21-jdk AS builder

# Instala o protoc e wget
RUN apt-get update && apt-get install -y protobuf-compiler wget && rm -rf /var/lib/apt/lists/*

WORKDIR /build

# Baixa a biblioteca do Protobuf para Java (Runtime)
RUN wget -q https://repo1.maven.org/maven2/com/google/protobuf/protobuf-java/3.25.1/protobuf-java-3.25.1.jar -O protobuf.jar

# Copia os arquivos conforme a estrutura solicitada
COPY ./common ./common
COPY ./sensor_java/sensor.java .

# 1. Gera as classes Java a partir do .proto compartilhado
RUN protoc -I=./common --java_out=. ./common/messages.proto

# 2. Compila o sensor.java incluindo o JAR do Protobuf e as classes geradas
RUN javac -cp protobuf.jar sensor.java smartcity/Messages.java

# Estágio de Runtime (Imagem leve)
FROM eclipse-temurin:21-jre

WORKDIR /app

COPY --from=builder /build/protobuf.jar .
COPY --from=builder /build/*.class .
COPY --from=builder /build/smartcity ./smartcity

# Executa o Java apontando para o Classpath correto
CMD ["java", "-cp", ".:protobuf.jar", "sensor"]