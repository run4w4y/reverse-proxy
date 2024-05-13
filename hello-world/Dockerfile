FROM rust:1.70 as builder

WORKDIR /usr/src/hello-world
COPY . .

RUN cargo install --path .

FROM debian:buster-slim as runner
RUN apt-get update && apt-get install -y openssl libssl-dev pkg-config && rm -rf /var/lib/apt/lists/*

WORKDIR /
COPY --from=builder /usr/local/cargo/bin/hello-world /usr/local/bin/hello-world

CMD ["hello-world"]
