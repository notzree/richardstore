# Dockerfile.datanode
FROM golang:1.23
WORKDIR /app
COPY . .
# Build the binary directly to /app/bin
RUN mkdir -p /app/bin && \
    go build -v -o /app/bin/datanode ./cmd/datanode && \
    chmod +x /app/bin/datanode
CMD ["/app/bin/datanode"]
