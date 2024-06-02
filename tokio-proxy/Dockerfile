FROM rust:1.78 as builder

WORKDIR /usr/src/tokio-proxy
COPY . .

RUN cargo install --path .

FROM debian:stable-20240513 as runner
RUN apt-get update && apt-get install -y openssl libssl-dev pkg-config libc6 && rm -rf /var/lib/apt/lists/*

WORKDIR /
COPY --from=builder /usr/local/cargo/bin/tokio-proxy /usr/local/bin/tokio-proxy

CMD ["tokio-proxy"]
