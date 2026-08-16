ARG GO_IMAGE=golang:1.26.5-alpine3.23@sha256:622e56dbc11a8cfe87cafa2331e9a201877271cbff918af53d3be315f3da88cc
ARG RUNTIME_IMAGE=alpine:3.23@sha256:fd791d74b68913cbb027c6546007b3f0d3bc45125f797758156952bc2d6daf40

FROM ${GO_IMAGE} AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -o /out/streamd ./cmd/streamd \
 && CGO_ENABLED=0 go build -trimpath -o /out/proxy-init ./test/ha/cmd/proxy-init

FROM ${RUNTIME_IMAGE} AS streamd
COPY --from=build /out/streamd /usr/local/bin/streamd
ENTRYPOINT ["/usr/local/bin/streamd"]

FROM ${RUNTIME_IMAGE} AS proxy-init
COPY --from=build /out/proxy-init /usr/local/bin/proxy-init
ENTRYPOINT ["/usr/local/bin/proxy-init"]

FROM build AS test-runner
ENTRYPOINT ["go", "test", "-tags=integration", "./test/ha", "-count=1", "-v"]
