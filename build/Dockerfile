# One image per command: docker build --build-arg CMD=collector|querier|compactor -f build/Dockerfile .
ARG CMD

FROM golang:1.25 AS build
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
