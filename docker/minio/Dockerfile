# Official community container registries removed historical MinIO images.
# Build the pinned upstream source instead of using an unofficial mirror.
FROM golang:1.24.2-bookworm AS build
ARG MINIO_VERSION=RELEASE.2025-04-22T22-12-26Z
WORKDIR /src
RUN curl -fsSL --retry 5 "https://codeload.github.com/minio/minio/tar.gz/refs/tags/${MINIO_VERSION}" \
    | tar -xz --strip-components=1
RUN CGO_ENABLED=0 go build -trimpath \
    -ldflags="-s -w -X github.com/minio/minio/cmd.ReleaseTag=${MINIO_VERSION} -X github.com/minio/minio/cmd.Version=2025-04-22T22:12:26Z" \
    -o /minio .

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates curl \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --uid 10002 --create-home minio \
    && mkdir /data && chown minio:minio /data
COPY --from=build /minio /usr/local/bin/minio
USER minio
EXPOSE 9000 9001
ENTRYPOINT ["minio"]
