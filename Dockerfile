FROM rust:1-trixie AS builder
WORKDIR /s3sync
COPY . ./
RUN git config --global --add safe.directory /s3sync \
&& cargo build --release

FROM debian:trixie-slim
RUN apt-get update \
&& apt-get install --no-install-recommends -y ca-certificates \
&& apt-get clean \
&& rm -rf /var/lib/apt/lists/*

COPY --from=builder /s3sync/target/release/s3sync /usr/local/bin/s3sync

RUN useradd -m -s /bin/bash s3sync
USER s3sync
WORKDIR /home/s3sync/
ENTRYPOINT ["/usr/local/bin/s3sync"]
