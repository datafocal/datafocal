# One image per command, built by make image:
#   docker build --build-arg GO_VERSION=1.26 --build-arg CMD=collector|querier|compactor -f build/Dockerfile .
# GO_VERSION is go.mod's go line to the minor, so the image builds with the Go the module
# requires; it has no default, so a build without it fails rather than drifting from go.mod.
ARG GO_VERSION
ARG CMD

FROM golang:${GO_VERSION} AS build
ARG CMD
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY cmd ./cmd
COPY internal ./internal
COPY web/*.go ./web/
COPY web/dist ./web/dist
RUN CGO_ENABLED=0 go build -trimpath -o /out/app ./cmd/${CMD}

# The querier and compactor run as nonroot. The collector reads node logs, so its user is left to the
# deployment that mounts them.
FROM gcr.io/distroless/static-debian12 AS collector
COPY --from=build /out/app /app
ENTRYPOINT ["/app"]

FROM gcr.io/distroless/static-debian12:nonroot AS querier
COPY --from=build /out/app /app
ENTRYPOINT ["/app"]

FROM gcr.io/distroless/static-debian12:nonroot AS compactor
COPY --from=build /out/app /app
ENTRYPOINT ["/app"]

FROM ${CMD}
