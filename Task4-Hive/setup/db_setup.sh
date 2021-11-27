#!/usr/bin/env setup

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

hive -f ../hive_scripts/create_databases.hql

hive -f ../hive_scripts/landing_fund_db/DDL/tables/create_landing_fund_table.hql
hive -f ../hive_scripts/landing_fund_db/DDL/tables/create_audit_table.hql
hive -f ../hive_scripts/landing_fund_db/DDL/tables/create_tmp_fund_table.hql

hive -f ../hive_scripts/stg_fund_db/DDL/tables/create_stg_fund_table.hql
hive -f ../hive_scripts/stg_fund_db/DDL/tables/create_audit_table.hql

hive -f ../hive_scripts/fund_db/DDL/tables/create_monthly_table.hql
hive -f ../hive_scripts/fund_db/DDL/tables/create_audit_table.hql

hplsql -f ../hive_scripts/stg_fund_db/DDL/procedures/create_clear_fund_procedure.hql
hplsql -f ../hive_scripts/fund_db/DDL/procedures/create_monthly_average_fund_procedure.hql

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
