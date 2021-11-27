#!/bin/bash


apt-get update -y
apt-get install -y docker.io

docker build -f ./hive_metastorage/Dockerfile . -t postgresql-hms

docker build -f ./hadoop_cluster/Dockerfile . -t hadoop_cluster