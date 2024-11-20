FROM groovy:4.0-jdk21-alpine

COPY loxone2influxdb.groovy /loxone2influxdb.groovy
COPY run.sh /run.sh
RUN [ "/run.sh", "grape" ]
ENTRYPOINT [ "/run.sh" ]

