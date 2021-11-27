#!/usr/bin/env bash

READ_CONFIGS=""
ACCOUNT=""
USERNAME=""

while [ "$1" ]; do
  case "$1" in
  -r) READ_CONFIGS="true" ;;
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

snowsql <<<"CREATE OR REPLACE DATABASE audit_db;
            CREATE OR REPLACE DATABASE landing_fund_db;
            CREATE OR REPLACE DATABASE stg_fund_db;
            CREATE OR REPLACE DATABASE fund_db;"

for file in $(find SQL -type f -name "*"); do
  snowsql -a ${ACCOUNT} -u ${USERNAME} -f ${file}
done

echo "SETUP COMPLETE"
