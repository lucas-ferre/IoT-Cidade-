FROM node:20-alpine

WORKDIR /app

# Instala dependências (protobufjs) primeiro p/ aproveitar cache de camada
COPY ./sensor_node/package.json ./
RUN npm install --omit=dev

# Código do sensor + contrato Protobuf centralizado (carregado em runtime)
COPY ./sensor_node/ ./
COPY ./common/messages.proto ./

# Porta de controle TCP (atuação remota)
EXPOSE 5010

CMD ["node", "sensor.js"]
