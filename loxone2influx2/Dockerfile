FROM python:3-alpine

WORKDIR /app

RUN pip install --no-cache-dir influxdb

COPY *.py /app

CMD [ "python", "./Loxone2InfluxDB.py" ]