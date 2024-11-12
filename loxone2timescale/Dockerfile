FROM python:3-slim
#FROM python:3

COPY requirements.txt /
RUN pip install --no-cache-dir -r /requirements.txt
COPY loxone2timescale.py /

ENTRYPOINT [ "python3", "/loxone2timescale.py" ]
