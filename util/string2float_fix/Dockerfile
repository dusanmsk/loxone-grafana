FROM ubuntu

RUN apt-get update && apt-get install -y groovy bash influxdb-client

COPY convert.groovy /convert.groovy

ENTRYPOINT [ "groovy", "/convert.groovy" ]

