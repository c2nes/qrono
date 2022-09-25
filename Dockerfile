FROM rust:1.64-slim AS builder
WORKDIR /app
COPY . .
RUN apt-get update \
 && apt-get install -y make protobuf-compiler
RUN cargo build --release --bin qrono

FROM debian:11-slim
COPY --from=builder /app/target/release/qrono /usr/local/bin/qrono
# RESP (Redis protocol) interface
EXPOSE 16379
# HTTP interface
EXPOSE 16780
# gRPC interface
EXPOSE 16381
# Data directory
VOLUME /var/lib/qrono
ENV RUST_LOG=info
CMD /usr/local/bin/qrono --rayon --data /var/lib/qrono --working-set-stripes=4
