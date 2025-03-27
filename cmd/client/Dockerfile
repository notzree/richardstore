# Dockerfile.datanode
FROM golang:1.23
WORKDIR /app
COPY . .
# Build the binary directly to /app/bin
RUN mkdir -p /app/bin && \
    go build -v -o /app/bin/client ./cmd/client && \
    chmod +x /app/bin/client
CMD ["/app/bin/client"]
