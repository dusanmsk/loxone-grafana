#!/bin/bash
echo "use -d to run in detached mode"
source common     
files=$(get_compose_files)
docker-compose $files up  $@
