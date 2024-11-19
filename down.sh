#!/bin/bash
source common     
files=$(get_compose_files)
docker-compose $files down $@
