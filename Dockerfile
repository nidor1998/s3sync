FROM rust:latest as builder
WORKDIR /s3sync
COPY . ./
RUN cargo build --release


FROM debian:bookworm-slim
RUN apt-get update \
&& apt-get install --no-install-recommends -y ca-certificates \
&& apt-get clean \
&& rm -rf /var/lib/apt/lists/*

COPY --from=builder /s3sync/target/release/s3sync /usr/local/bin/s3sync

RUN adduser s3sync
USER s3sync
WORKDIR /home/s3sync/
ENTRYPOINT ["/usr/local/bin/s3sync"]
