FROM python:3-slim
#FROM python:3

COPY requirements.txt /
RUN pip install --no-cache-dir -r /requirements.txt
#RUN apt-get update && apt-get install -y python3-debugpy
RUN mkdir -p /app
COPY run.sh /app
COPY loxone2timescale.py /app

ENTRYPOINT [ "/app/run.sh" ]
