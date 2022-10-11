# syntax=docker/dockerfile:1
FROM golang:alpine
WORKDIR /app
ENV GOLANG_RUNNING_IN_CONTAINER=true
COPY go.mod go.sum ./
RUN go mod download && go mod verify
COPY . .
RUN mkdir -p ./certs/
COPY certs/ ./certs/
RUN go build -v -o /app/consumer ./...
ENTRYPOINT ["/app/consumer"]
