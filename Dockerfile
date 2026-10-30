FROM rust:1-bookworm AS build
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY src ./src
RUN cargo build --release --locked --bins

FROM debian:bookworm-slim
RUN useradd --system --uid 10001 --home-dir /data --shell /usr/sbin/nologin kv \
    && mkdir /data && chown kv /data
COPY --from=build /src/target/release/main_shard /src/target/release/client \
     /src/target/release/chaos /src/target/release/bench /usr/local/bin/
USER kv
WORKDIR /data
ENV SHARD_DATA_DIR=/data/shard_data \
    SHARD_METRICS_BIND=0.0.0.0
VOLUME /data
EXPOSE 9200 7200 9300
HEALTHCHECK --interval=10s --timeout=3s --start-period=10s --retries=3 \
    CMD bash -c 'exec 3<>/dev/tcp/127.0.0.1/${SHARD_METRICS_PORT:-9300} && printf "GET / HTTP/1.0\r\n\r\n" >&3 && head -n1 <&3 | grep -q 200'
ENTRYPOINT ["main_shard"]
