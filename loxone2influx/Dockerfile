FROM groovy:jre8-alpine

COPY loxone2influx.groovy /loxone2influx.groovy
RUN  chmod a=rx /loxone2influx.groovy

ENTRYPOINT [ "groovy", "/loxone2influx.groovy" ]

