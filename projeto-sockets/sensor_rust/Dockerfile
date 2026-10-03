FROM rust:1.90-bookworm AS builder

WORKDIR /src/sensor_rust
COPY ./sensor_rust/Cargo.toml ./sensor_rust/Cargo.lock ./sensor_rust/build.rs ./
COPY ./sensor_rust/src ./src
COPY ./sensor_rust/tests ./tests
COPY ./common/messages.proto /src/common/messages.proto

RUN cargo test --locked \
    && cargo build --release --locked \
    && mkdir -p /out \
    && cp target/release/sensor-water /out/sensor-water

FROM gcr.io/distroless/cc-debian12:nonroot
WORKDIR /app
COPY --from=builder --chown=65532:65532 /out/sensor-water /app/sensor-water
USER 65532:65532
EXPOSE 5008
HEALTHCHECK --interval=10s --timeout=3s --start-period=5s --retries=3 \
    CMD ["/app/sensor-water", "healthcheck"]
ENTRYPOINT ["/app/sensor-water"]
