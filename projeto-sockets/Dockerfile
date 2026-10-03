ARG TARGET_SERVICE=gateway

FROM python:3.11-slim AS gateway
RUN apt-get update && apt-get install -y \
    protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY ./gateway/requirements.txt ./gateway/requirements.txt
RUN python -m pip install --no-cache-dir --requirement ./gateway/requirements.txt \
    && python -m pip check
COPY ./gateway ./gateway
COPY ./common ./common
RUN protoc -I=./common --python_out=./gateway ./common/messages.proto
WORKDIR /app/gateway
RUN mkdir -p /app/gateway/db
CMD ["python", "main.py"]

FROM ubuntu:24.04 AS sensor_clima
RUN apt-get update && apt-get install -y \
    build-essential \
    libprotobuf-c-dev \
    protobuf-c-compiler \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY ./sensor_c .
COPY ./common/messages.proto .
RUN protoc-c --c_out=. messages.proto
RUN gcc -std=c11 -Wall -Wextra -Werror test_aqi.c -lm -o test_aqi && ./test_aqi
RUN gcc -std=c11 -Wall -Wextra -Werror test_environment.c -lm -o test_environment && ./test_environment
RUN gcc -o sensor_c sensor.c messages.pb-c.c -lprotobuf-c -lm -pthread
CMD ["./sensor_c"]

FROM ubuntu:24.04 AS sensor_posto
RUN apt-get update && apt-get install -y \
    lua5.4 \
    liblua5.4-dev \
    luarocks \
    build-essential \
    protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*
RUN luarocks --lua-version=5.4 install lua-protobuf && \
    luarocks --lua-version=5.4 install luasocket    && \
    luarocks --lua-version=5.4 install luaposix
WORKDIR /app
COPY ./sensor_lua/sensor.lua .
COPY ./sensor_lua/lamp_metrics.lua ./sensor_lua/test_lamp_metrics.lua ./
RUN lua5.4 test_lamp_metrics.lua
COPY ./common/messages.proto .
RUN protoc --descriptor_set_out=messages.pb messages.proto
CMD ["sh", "-c", "eval $(luarocks --lua-version=5.4 path) && lua5.4 sensor.lua"]

FROM eclipse-temurin:21-jdk AS sensor_java_builder
RUN apt-get update && apt-get install -y protobuf-compiler wget && rm -rf /var/lib/apt/lists/*
WORKDIR /build
RUN wget -q https://repo1.maven.org/maven2/com/google/protobuf/protobuf-java/3.25.1/protobuf-java-3.25.1.jar -O protobuf.jar
COPY ./common ./common
COPY ./sensor_java/sensor.java .
COPY ./sensor_java/TrafficSample.java ./sensor_java/TrafficSampleTest.java ./
RUN javac TrafficSample.java TrafficSampleTest.java && java TrafficSampleTest
RUN protoc -I=./common --java_out=. ./common/messages.proto
RUN javac -cp protobuf.jar sensor.java TrafficSample.java smartcity/Messages.java

FROM eclipse-temurin:21-jre AS sensor_java
WORKDIR /app
COPY --from=sensor_java_builder /build/protobuf.jar .
COPY --from=sensor_java_builder /build/*.class .
COPY --from=sensor_java_builder /build/smartcity ./smartcity
CMD ["java", "-cp", ".:protobuf.jar", "sensor"]

FROM python:3.11-slim AS sensor_camera
RUN apt-get update && apt-get install -y \
    protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY ./sensor_python/requirements.txt ./sensor_python/requirements.txt
RUN python -m pip install --no-cache-dir --requirement ./sensor_python/requirements.txt \
    && python -m pip check
COPY ./sensor_python ./sensor_python
COPY ./common ./common
RUN protoc -I=./common --python_out=./sensor_python ./common/messages.proto
WORKDIR /app/sensor_python
CMD ["python", "sensor.py"]

FROM golang:1.23-bookworm AS sensor_estacionamento_builder
ARG PROTOC_GEN_GO_VERSION=v1.36.6
RUN apt-get update \
    && apt-get install -y --no-install-recommends protobuf-compiler ca-certificates \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /src/sensor_go
COPY ./sensor_go/go.mod ./sensor_go/go.sum ./
RUN go mod download \
    && go mod verify \
    && GOBIN=/usr/local/bin go install google.golang.org/protobuf/cmd/protoc-gen-go@${PROTOC_GEN_GO_VERSION}
COPY ./sensor_go/main.go ./sensor_go/main_test.go ./
COPY ./common/messages.proto /src/common/messages.proto
RUN mkdir -p ./proto \
    && protoc \
        --proto_path=/src/common \
        --go_out=./proto \
        --go_opt=paths=source_relative \
        /src/common/messages.proto \
    && go test -race -mod=readonly ./... \
    && CGO_ENABLED=0 GOOS=linux go build -mod=readonly \
        -trimpath \
        -ldflags="-s -w" \
        -o /out/sensor-go \
        .

FROM gcr.io/distroless/static-debian12:nonroot AS sensor_estacionamento
WORKDIR /app
COPY --from=sensor_estacionamento_builder --chown=65532:65532 /out/sensor-go /app/sensor-go
EXPOSE 5007
USER 65532:65532
HEALTHCHECK --interval=10s --timeout=3s --start-period=5s --retries=3 \
    CMD ["/app/sensor-go", "healthcheck"]
ENTRYPOINT ["/app/sensor-go"]

FROM python:3.11-slim AS dashboard
RUN apt-get update && apt-get install -y \
    protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY ./client/requirements.txt ./client/requirements.txt
RUN python -m pip install --no-cache-dir --requirement ./client/requirements.txt \
    && python -m pip check
COPY ./client ./client
COPY ./common ./common
RUN protoc -I=./common --python_out=./client ./common/messages.proto
WORKDIR /app/client
EXPOSE 8501
CMD ["streamlit", "run", "app.py", "--server.port=8501", "--server.address=0.0.0.0"]

FROM python:3.11-slim AS hubs_runtime
RUN apt-get update && apt-get install -y --no-install-recommends protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY ./hubs/requirements.txt ./hubs/requirements.txt
RUN python -m pip install --no-cache-dir -r hubs/requirements.txt && python -m pip check
COPY ./hubs ./hubs
COPY ./common ./common
RUN protoc -I=common --python_out=. common/messages.proto \
    && useradd --system --uid 10001 hub
USER hub

FROM hubs_runtime AS hub_sensores
CMD ["python", "-m", "hubs.main", "sensores"]

FROM hubs_runtime AS hub_acesso
CMD ["python", "-m", "hubs.main", "acesso"]

FROM python:3.11-slim AS sensor_invasor
RUN apt-get update && apt-get install -y --no-install-recommends protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY ./sensor_intruder/requirements.txt ./sensor_intruder/requirements.txt
RUN python -m pip install --no-cache-dir -r sensor_intruder/requirements.txt && python -m pip check
COPY ./sensor_intruder ./sensor_intruder
COPY ./common ./common
RUN protoc -I=common --python_out=. common/messages.proto \
    && useradd --system --uid 10002 intruder
USER intruder
CMD ["python", "-m", "sensor_intruder.main"]

FROM rust:1.90-bookworm AS sensor_agua_builder
WORKDIR /src/sensor_rust
COPY ./sensor_rust/Cargo.toml ./sensor_rust/Cargo.lock ./sensor_rust/build.rs ./
COPY ./sensor_rust/src ./src
COPY ./sensor_rust/tests ./tests
COPY ./common/messages.proto /src/common/messages.proto
RUN cargo test --locked && cargo build --release --locked \
    && mkdir -p /out && cp target/release/sensor-water /out/sensor-water

FROM gcr.io/distroless/cc-debian12:nonroot AS sensor_agua
WORKDIR /app
COPY --from=sensor_agua_builder --chown=65532:65532 /out/sensor-water /app/sensor-water
USER 65532:65532
EXPOSE 5008
HEALTHCHECK --interval=10s --timeout=3s --start-period=5s --retries=3 \
    CMD ["/app/sensor-water", "healthcheck"]
ENTRYPOINT ["/app/sensor-water"]

FROM node:22-bookworm-slim AS sensor_lixeiras
WORKDIR /app/sensor_typescript
COPY ./sensor_typescript ./
RUN node --experimental-strip-types --test tests/*.test.ts
USER node
HEALTHCHECK --interval=10s --timeout=3s --start-period=5s --retries=3 \
    CMD node -e "try { process.kill(1, 0); } catch { process.exit(1); }"
CMD ["node", "--experimental-strip-types", "src/main.ts"]

FROM ${TARGET_SERVICE} AS final
