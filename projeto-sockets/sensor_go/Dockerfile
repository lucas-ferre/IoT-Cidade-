FROM golang:1.23-bookworm AS builder

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

FROM gcr.io/distroless/static-debian12:nonroot

WORKDIR /app

COPY --from=builder --chown=65532:65532 /out/sensor-go /app/sensor-go

EXPOSE 5007

USER 65532:65532

HEALTHCHECK --interval=10s --timeout=3s --start-period=5s --retries=3 \
    CMD ["/app/sensor-go", "healthcheck"]

ENTRYPOINT ["/app/sensor-go"]
