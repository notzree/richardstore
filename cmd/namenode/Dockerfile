# Dockerfile.namenode
FROM golang:1.23
WORKDIR /app
COPY . .
# Build the binary directly to /app/bin
RUN mkdir -p /app/bin && \
    go build -v -o /app/bin/namenode ./cmd/namenode && \
    chmod +x /app/bin/namenode


CMD ["/app/bin/namenode"]
