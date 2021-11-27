#!/usr/bin/env bash

hdfs dfs -rm -r -skipTrash /tmp/data
hdfs dfs -rm -r -skipTrash /tmp/fund
hdfs dfs -rm -r -skipTrash /tmp/stg_fund
hdfs dfs -rm -r -skipTrash /tmp/audit
hdfs dfs -rm -r -skipTrash /tmp/landing_fund
hdfs dfs -rm -r -skipTrash /tmp/tmp_file
hdfs dfs -rm -r -skipTrash /tmp/test

hdfs dfs -mkdir /tmp/data
hdfs dfs -mkdir /tmp/fund
hdfs dfs -mkdir /tmp/stg_fund
hdfs dfs -mkdir /tmp/audit
hdfs dfs -mkdir /tmp/landing_fund
