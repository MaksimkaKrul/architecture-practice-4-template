FROM golang:1.24 AS build

WORKDIR /go/src/practice-4
COPY . .

RUN go test ./...
ENV CGO_ENABLED=0
RUN go install ./cmd/...

FROM alpine:latest
WORKDIR /opt/practice-4
COPY --from=build /go/bin/client .
COPY --from=build /go/bin/lb .
COPY --from=build /go/bin/server .
COPY --from=build /go/bin/stats .
COPY entry.sh .

RUN chmod +x /opt/practice-4/entry.sh && \
    ls -la /opt/practice-4/

ENTRYPOINT ["/opt/practice-4/entry.sh"]