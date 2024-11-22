# influx to questdb migration howto

- create temp virtualenv
  - python3 -m venv /tmp/venv
  - source /tmp/venv/bin/activate
- install required libraries
  - pip3 install -r requirements-migrate.txt
- create migrate.sh script, use migrate.sh.example as reference