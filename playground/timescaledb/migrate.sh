#!/bin/bash

# influx -database 'loxone' -host 'localhost' -port '8086' -execute "show measurements"

export PGPORT=5432
export PGDATABASE=loxone
export PGUSER=loxone
export PGPASSWORD=loxone

INFLUX_HOST=locahost
INFLUX_PORT=8086
INFLUX_USER=loxone
INFLUX_PASSWORD=loxone

#./outflux migrate loxone
./outflux schema-transfer loxone
