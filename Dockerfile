FROM golang:1.16 AS debug-tools
RUN go install github.com/c2nes/jtopthreads@latest
RUN go install github.com/c2nes/grep-stackdump@latest

FROM adoptopenjdk:11-jdk-hotspot

# Debug utilities
COPY --from=debug-tools /go/bin/jtopthreads /usr/local/bin/jtopthreads
COPY --from=debug-tools /go/bin/grep-stackdump /usr/local/bin/jtopthreads

ADD target/qrono-server.jar /app/qrono-server.jar
ADD gateway/gateway /app/gateway
ADD run-server /app/run-server
RUN mkdir /app/config

# RESP (Redis protocol) interface
EXPOSE 16379
# HTTP interface
EXPOSE 16780
# gRPC interface
EXPOSE 16381

VOLUME /var/lib/qrono
WORKDIR /app

CMD /app/run-server
