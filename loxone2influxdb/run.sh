#!/bin/sh
groovy -Dgroovy.grape.report.downloads=true -Dlog4j2.level=${LOXONE2INFLUXDB_LOGLEVEL} /loxone2influxdb.groovy $@
