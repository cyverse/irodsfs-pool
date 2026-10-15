FROM golang:1.25-bookworm AS builder

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download

COPY . .
ARG LDFLAGS=""
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="${LDFLAGS}" -o /irodsfs-pool ./cmd

FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && \
    rm -rf /var/lib/apt/lists/*

COPY --from=builder /irodsfs-pool /usr/local/bin/irodsfs-pool

RUN mkdir -p /data/irodsfs-pool

EXPOSE 12020 12021

ENTRYPOINT ["irodsfs-pool"]
CMD ["start", "-c", "/etc/irodsfs-pool/config.yaml"]
