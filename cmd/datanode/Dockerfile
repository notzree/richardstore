# Base image
FROM golang:1.23

# Set working directory
WORKDIR /app

COPY go.mod go.sum Makefile ./

# Copy the rest of the application code
COPY . .

# Run the application using make run
CMD ["make", "run-datanode"]
