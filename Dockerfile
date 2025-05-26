FROM golang:1.24 AS build

WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .

# RUN go test ./...

RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go install ./cmd/server ./cmd/lb ./cmd/db ./cmd/stats

RUN ls -l /go/bin

FROM alpine:latest AS server
WORKDIR /opt/practice-4
COPY --from=build /go/bin/server .
# COPY entry.sh .
RUN ls -la /opt/practice-4/
RUN chmod +x /opt/practice-4/server
CMD ["./server"]

FROM alpine:latest AS lb
WORKDIR /opt/practice-4
COPY --from=build /go/bin/lb .
# COPY entry.sh .
RUN ls -la /opt/practice-4/
RUN chmod +x /opt/practice-4/lb
CMD ["./lb"]

FROM alpine:latest AS stats
WORKDIR /opt/practice-4
COPY --from=build /go/bin/stats .
CMD ["./stats"]

FROM alpine:latest AS db
WORKDIR /opt/practice-4
COPY --from=build /go/bin/db .
# COPY entry.sh .
RUN ls -la /opt/practice-4/
RUN chmod +x /opt/practice-4/db
CMD ["./db"]

FROM golang:1.24 AS test
WORKDIR /go/src/practice-4
COPY . .
ENV INTEGRATION_TEST=1
ENTRYPOINT ["go", "test", "-v", "./integration"]