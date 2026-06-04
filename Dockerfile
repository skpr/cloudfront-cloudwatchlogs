FROM scratch

COPY --from=alpine:latest /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/

ARG TARGETPLATFORM
COPY $TARGETPLATFORM/cloudfront-cloudwatchlogs /usr/sbin/cloudfront-cloudwatchlogs

ENTRYPOINT ["/usr/sbin/cloudfront-cloudwatchlogs"]
CMD ["--help"]
