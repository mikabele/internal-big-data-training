#!/usr/bin/env bash

DATA_DIR=""
TYPE_OF_LOAD=""
MARKET=""
READ_CONFIGS=""
OPTION=""
ACCOUNT=""
USERNAME=""

while [ "$1" ]; do
  case "$1" in
  -t | --type)
    TYPE_OF_LOAD=$2
    shift
    ;;
  -d | --data_dir)
    DATA_DIR=$2
    shift
    ;;
  -m | --market)
    MARKET=$2
    shift
    ;;
  -r) READ_CONFIGS="true" ;;
  -o)
    OPTION=$2
    shift
    ;;
  -a)
    ACCOUNT=$2
    shift
    ;;
  -u)
    USERNAME=$2
    shift
    ;;
  esac
  shift
done

if [[ $READ_CONFIGS == "true" ]]; then
  . ./configs/snowflake_configs.txt
fi

snowsql -a ${ACCOUNT} -u ${USERNAME} <<<"PUT 'file://${DATA_DIR}/*' @landing_fund_db.public.landing_fund_stage"

if [[ $OPTION == "run" ]]; then
  sbt "run -m ${MARKET} -d ${DATA_DIR} -t ${TYPE_OF_LOAD}"
else
  if [[ $OPTION == "compile" ]]; then
    sbt "compile"
  else
    echo "WRONG OPTION!"
    exit 1
  fi
fi
