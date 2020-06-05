quickstart:

1. edit node-lox-mqtt-gateway/config.json and set loxone address, username and password in "miniserver" section
2. execute ./rebuild.sh
3. if ok, execute ./run.sh. If everything works fine, you should see some messages from loxone in loxone2influx image (Stored ..., Skipped ...)
4. stop by CTRL+C then start by ./start.sh
5. go to http://localhost:3000, admin:admin, configure new influxdb datasource as:
    name:influx, default:yes, type:InfluxDB, url:http://influxdb:8086, access:proxy, noauth, database:loxone, nouser, save and test (must be green)
6. grafana is ready, go ahead and create some graphs
