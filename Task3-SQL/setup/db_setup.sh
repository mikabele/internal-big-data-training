#!/usr/bin/env bash

DATA_DIR=""
INCREMENTAL=false
WITHOUT_LOAD=false
TASK=""
MARKET=""

while [ "$1" ]; do
  case "$1" in
  -d | --data_dir)
    DATA_DIR=$2
    shift
    ;;
  -i | --incremental) INCREMENTAL=true ;;
  -f | --full) INCREMENTAL=false ;;
  -w | --without_load) WITHOUT_LOAD=true ;;
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

source ../Configs/configs.ini

mysql -u $USER -p$USER_PASSWORD -h $HOST -P $PORT -e "SET GLOBAl local_infile=1"
mysql -u $USER -p$USER_PASSWORD -h $HOST -P $PORT -e "SET GLOBAl time_zone='${TIMEZONE}'"

mysql -u $USER -p$USER_PASSWORD -h $HOST -P $PORT <../SQL/create_databases.sql

mysql -u $USER -p$USER_PASSWORD -h $HOST -P $PORT -D landing_fund_db <../SQL/landing_fund_db/DDL/Tables/create_landing_fund_table.sql
mysql -u $USER -p$USER_PASSWORD -h $HOST -P $PORT -D landing_fund_db <../SQL/landing_fund_db/DDL/Tables/create_audit_table.sql

mysql -u $USER -p$USER_PASSWORD -h $HOST -P $PORT -D stg_fund_db <../SQL/stg_fund_db/DDL/Tables/create_stg_fund_table.sql
mysql -u $USER -p$USER_PASSWORD -h $HOST -P $PORT -D stg_fund_db <../SQL/stg_fund_db/DDL/Tables/create_audit_table.sql

mysql -u $USER -p$USER_PASSWORD -h $HOST -P $PORT -D fund_db <../SQL/fund_db/DDL/Tables/create_monthly_table.sql
mysql -u $USER -p$USER_PASSWORD -h $HOST -P $PORT -D fund_db <../SQL/fund_db/DDL/Tables/create_audit_table.sql

mysql -u $USER -p$USER_PASSWORD -h $HOST -P $PORT -D stg_fund_db <../SQL/stg_fund_db/DDL/Procedures/create_clear_fund_procedure.sql
mysql -u $USER -p$USER_PASSWORD -h $HOST -P $PORT <../SQL/fund_db/DDL/Procedures/create_monthly_average_fund_procedure.sql

if [[ $WITHOUT_LOAD == true ]]; then
  exit 0
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

if [[ $INCREMENTAL == true ]]; then
  bash manage_loading_process.sh -d ${DATA_DIR} -t ${TASK} -m ${MARKET} -i
else
  bash manage_loading_process.sh -d ${DATA_DIR} -t ${TASK} -m ${MARKET} -f
fi
