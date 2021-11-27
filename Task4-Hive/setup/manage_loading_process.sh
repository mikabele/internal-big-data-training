#!/usr/bin/env bash

DATA_DIR=""
INCREMENTAL=0
FULL=0
TASK=""
MARKET=""

while [ "$1" ]; do
  case "$1" in
  -d | --data_dir)
    DATA_DIR=$2
    shift
    ;;
  -i | --incremental) INCREMENTAL=1 ;;
  -f | --full) FULL=1 ;;
  -t | --task)
    TASK=$2
    shift
    ;;
  -m | --market)
    MARKET=$2
    shift
    ;;
  esac
  shift
done

if [[ $FULL -eq $INCREMENTAL ]]; then
  echo "Please, choose only one option of load"
  exit 1
fi

if [[ -z $DATA_DIR ]]; then
  echo "Please, enter data directory"
  exit 1
fi

if [[ -z $TASK ]]; then
  echo "Please, enter task"
  exit 1
fi

if [[ -z $MARKET ]]; then
  echo "Please, enter market"
  exit 1
fi

pip3 install -r requirements.txt

if [[ $INCREMENTAL -eq 1 ]]; then
  python3 load_data.py -d ${DATA_DIR} -t ${TASK} -m ${MARKET} -i
  hplsql -e "INCLUDE '../hive_scripts/stg_fund_db/DDL/procedures/create_clear_fund_procedure.hql'; CALL stg_fund_db.usp_clear_fund(FALSE);"
else
  python3 load_data.py -d ${DATA_DIR} -t ${TASK} -m ${MARKET} -f
  hplsql -e "INCLUDE '../hive_scripts/stg_fund_db/DDL/procedures/create_clear_fund_procedure.hql'; CALL stg_fund_db.usp_clear_fund(TRUE);"
fi
