#!/bin/bash
echo "use -d to run in detached mode"
source common     
docker compose up $@
