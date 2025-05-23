FROM golang:1.24 AS build

WORKDIR /go/src/practice-4
COPY . .

RUN go test ./...

RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go install ./cmd/...

RUN ls -l /go/bin

FROM alpine:latest AS server
WORKDIR /opt/practice-4
COPY --from=build /go/bin/server .
COPY entry.sh .
CMD ["./entry.sh", "server"]

FROM alpine:latest AS lb
WORKDIR /opt/practice-4
COPY --from=build /go/bin/lb .
COPY entry.sh .
CMD ["./entry.sh", "lb"]

FROM alpine:latest AS stats
WORKDIR /opt/practice-4
COPY --from=build /go/bin/stats .
CMD ["./stats"]

FROM golang:1.24 AS test
WORKDIR /go/src/practice-4
COPY . .
CMD ["go", "test", "-v", "./integration"]
