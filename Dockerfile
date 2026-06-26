# Multi-stage build of the CDC worker.
#
# The runtime is alpine (not distroless) so the compose healthcheck can use wget
# against /metrics. The binary is static (CGO disabled), so only the worker plus
# CA certs ship in the final image.
FROM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ARG VERSION=docker
RUN CGO_ENABLED=0 go build -ldflags "-X main.version=${VERSION}" -o /out/worker ./cmd/worker

FROM alpine:3.20
RUN apk add --no-cache wget ca-certificates
COPY --from=build /out/worker /usr/local/bin/worker
EXPOSE 9100
ENTRYPOINT ["/usr/local/bin/worker"]
