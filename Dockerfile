FROM debian:trixie AS builder
WORKDIR /s3sync
COPY . ./
RUN apt-get update \
&& apt-get install --no-install-recommends -y ca-certificates curl build-essential \
&& curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs >./rust_install.sh \
&& chmod +x rust_install.sh \
&& ./rust_install.sh -y \
&& . "$HOME/.cargo/env" \
&& rustup update \
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
