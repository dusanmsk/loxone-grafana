quickstart:

- create new empty directories /var/lib/loxone_monitoring/influxdb and /var/lib/loxone_monitoring/grafana
- edit node-lox-mqtt-gateway/config.json and set loxone address, username and password in "miniserver" section
- execute ./run.sh
- check for a while that everything builds and start up, you should see no errors
- you should see some messages from loxone in loxone2influx image (Stored ..., Skipped ...)
- go to http://localhost:3000, admin:admin, configure new influxdb datasource as:
    name:influx, default:yes, type:InfluxDB, url:http://influxdb:8086, access:proxy, noauth, database:loxone, nouser, save and test (must be green)
- grafana is ready, go ahead and create some graphs

