FROM ruby:3.3-slim

# protoc para gerar as classes Ruby a partir do contrato .proto
RUN apt-get update && apt-get install -y --no-install-recommends \
    protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Runtime Protobuf para Ruby (gem com binário pré-compilado p/ x86_64-linux)
RUN gem install google-protobuf -v 3.25.5

# Código do sensor + contrato centralizado
COPY ./sensor_ruby/ ./
COPY ./common/messages.proto ./

# Gera messages_pb.rb (módulo Smartcity) a partir do .proto
RUN protoc --ruby_out=. messages.proto

# Porta de controle TCP (atuação remota)
EXPOSE 5011

CMD ["ruby", "sensor.rb"]
