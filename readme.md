## Quickstart

### Steps:

- edit .env file and set DATA_DIR if required
- configure logger as described in this howto from section 2 https://github.com/budulinek/easy-loxone-influx/blob/master/1.UDP%20logger.md
- run `./rebuild.sh`
- run `./run.sh` and watch for messages like `influxdb_1        | [httpd] 172.18.0.4 - - [17/Apr/2024:11:38:50 +0000] "POST /write?db=loxone HTTP/1.1 " 204 0 "-" "python-requests/2.31.0" 0fd08ad9-fcaf-11ee-8002-0242ac120002 1547`
- if there are no errors, stop `CTRL+C` then run on background `./run.sh -d`
- open browser and go to `http://THIS_PC:3000`, configure new influxdb datasource as:
    - name: influx-loxone
    - default: yes
    - type: InfluxDB
    - url: `http://influxdb:8086`
    - no authentication
    - database: loxone
    - no user password
    - save and test
7. Grafana is ready, go ahead and create some graphs.



### Legacy versions of loxone (7, 8):

1. Create a dedicated user in loxone, assign permissions. Check in loxone web interface that new user see all controls you want to store into influx/grafana.
2. Edit `node-lox-mqtt-gateway/config.json` and set loxone address, username and password in "miniserver" section.
3. Execute `./rebuild.sh`.
4. If ok, execute `./run.sh`. If everything works fine, you should see some messages from loxone in loxone2influx image (Stored ..., Skipped ...).
5. Stop by CTRL+C then start by `./start.sh`.
6. Go to `http://localhost:3000`, admin:admin, configure new influxdb datasource as:
    - name: influx-loxone
    - default: yes
    - type: InfluxDB
    - url: `http://influxdb:8086`
    - no authentication
    - database: loxone
    - no user password
    - save and test
7. Grafana is ready, go ahead and create some graphs.

