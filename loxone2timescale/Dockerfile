FROM python:3-slim

COPY requirements.txt /
RUN pip install --no-cache-dir -r /requirements.txt
COPY loxone2timescale.py /

ENTRYPOINT [ "/usr/local/bin/python3", "/loxone2timescale.py" ]
