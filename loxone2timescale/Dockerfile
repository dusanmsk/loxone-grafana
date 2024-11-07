FROM python:3-slim

COPY loxone2timescale.py /
COPY requirements.txt /
RUN pip install --no-cache-dir -r /requirements.txt


ENTRYPOINT [ "/usr/local/bin/python3", "/loxone2timescale.py" ]
