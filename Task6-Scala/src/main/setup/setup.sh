#!/usr/bin/env bash

source ../configs/db_configs.txt

mysql -u${USER} -p${PASSWORD} -h${HOST} -P${PORT} <<<"CREATE DATABASE IF NOT EXISTS audit_db;"

for file in `find ../SQL -type f -name "*"`
do
   mysql -u${USER} -p${PASSWORD} -h${HOST} -P${PORT}  -f ${file}
   #echo ${file}
done