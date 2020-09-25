FROM adoptopenjdk:11-jdk-hotspot

ADD target/qrono-server.jar /app/qrono-server.jar
RUN mkdir /app/config

# RESP (Redis protocol) interface
EXPOSE 16379
# HTTP interface
EXPOSE 16780
# gRPC interface
EXPOSE 16381

VOLUME /var/lib/qrono
WORKDIR /app

CMD java -cp /app/qrono-server.jar:/app/config net.qrono.server.Main
