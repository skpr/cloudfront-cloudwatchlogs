FROM golang:latest AS build

COPY . /go/src/github.com/skpr/cloudfront-cloudwatchlogs
WORKDIR /go/src/github.com/skpr/cloudfront-cloudwatchlogs
RUN go get github.com/mitchellh/gox
RUN make build

FROM alpine:latest
RUN apk add --no-cache ca-certificates
COPY --from=build /go/src/github.com/skpr/cloudfront-cloudwatchlogs/bin/cloudfront-cloudwatchlogs_linux_amd64 /usr/sbin/cloudfront-cloudwatchlogs
RUN chmod +x /usr/sbin/cloudfront-cloudwatchlogs
ENTRYPOINT ["/usr/sbin/cloudfront-cloudwatchlogs"]
CMD ["--help"]
