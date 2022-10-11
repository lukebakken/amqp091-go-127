# syntax=docker/dockerfile:1
FROM golang:alpine
WORKDIR /usr/src/app
ENV GOLANG_RUNNING_IN_CONTAINER=true
COPY go.mod go.sum ./
RUN go mod download && go mod verify
COPY . .
RUN go build -v -o /usr/local/bin/consumer ./...
ENTRYPOINT ["/usr/local/bin/consumer"]
