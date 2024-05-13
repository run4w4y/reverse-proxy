FROM rust:1.70 as builder

WORKDIR /usr/src/tokio-proxy
COPY . .

RUN cargo install --path .

FROM debian:stable as runner
RUN apt-get update && apt-get install -y openssl libssl-dev pkg-config && rm -rf /var/lib/apt/lists/*

WORKDIR /
COPY --from=builder /usr/local/cargo/bin/tokio-proxy /usr/local/bin/tokio-proxy

CMD ["tokio-proxy"]
