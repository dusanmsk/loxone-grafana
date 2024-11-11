#!/bin/bash

# configuration variants:
#
# 1. run only loxone2mqtt
#   docker-compose up [-d] [--build] node-lox-mqtt-gw mosquitto
#
# 2. run loxone2mqtt with influxdb and grafana
#   docker-compose up [-d] [--build] node-lox-mqtt-gw mosquitto grafana influxdb loxone2influx
#
# 3. run loxone2mqtt with timescaledb and grafana
#       

docker-compose up $@
