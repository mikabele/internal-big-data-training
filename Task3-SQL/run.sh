#!/usr/bin/env bash

OPEN_CLOSE=0
CLOSE_CLOSE=0
MARKET=""
TASK=""
FULL="FALSE"
INCREMENTAL="FALSE"

while [ "$1" ]; do
  case "$1" in
  -cc | --close_close) CLOSE_CLOSE=1 ;;
  -oc | --open_close) OPEN_CLOSE=1 ;;
  --market)
    MARKET=$2
    shift
    ;;
  -t | --task)
    TASK=$2
    shift
    ;;
  -f | --full) FULL="TRUE" ;;
  -i | --incremental) INCREMENTAL="TRUE" ;;
  esac
  shift
done

if [[ $OPEN_CLOSE -eq $CLOSE_CLOSE ]]; then
  echo "Please, choose only one option of calculating average fund increase"
  exit 1
fi

if [[ $FULL == $INCREMENTAL ]]; then
  echo "Please, choose only one option of load"
  exit 1
fi

if [[ -z $MARKET ]]; then
  echo "Please, enter market"
  exit 1
fi

if [[ -z $TASK ]]; then
  echo "Please, enter task"
  exit 1
fi

source Configs/configs.ini

echo $'CALL usp_monthly_average_fund('${FULL}$',\''${OPEN_CLOSE}$'\',\''${MARKET}$'\',\''${TASK}$'\');' | mysql -u $USER -p$USER_PASSWORD -h $HOST -P $PORT --database fund_db
