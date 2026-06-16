FROM rust:1.76-bookworm AS builder

WORKDIR /usr/src/app
RUN apt-get update && apt-get install -y protobuf-compiler && rm -rf /var/lib/apt/lists/*

COPY aggregators/rust/ ./aggregators/rust/
COPY common/ ./common/

WORKDIR /usr/src/app/aggregators/rust
RUN cargo build --release

FROM debian:bookworm-slim
WORKDIR /app
COPY --from=builder /usr/src/app/aggregators/rust/target/release/aggregator-rust /usr/local/bin/aggregator-rust
CMD ["aggregator-rust"]
