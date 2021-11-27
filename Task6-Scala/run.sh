#!/usr/bin/env bash

DATA_DIR=""
TYPE_OF_LOAD=""
MARKET=""

while [ "$1" ]; do
  case "$1" in
  -t | --type) TYPE_OF_LOAD=$2
                shift;;
  -d | --data_dir) DATA_DIR=$2
                  shift;;
  -m | --market)
    MARKET=$2
    shift
    ;;
  esac
  shift
done

sbt "run -m ${MARKET} -d ${DATA_DIR} -t ${TYPE_OF_LOAD}"