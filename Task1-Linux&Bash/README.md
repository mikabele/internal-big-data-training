# Docker container with preinstalled environment

## Description

There user can create two docker containers on Linux distributives Ubuntu 20.04 and CentOS 7 with preinstalled Python 3.6, Java 11 (openjdk),
Scala 2.11 , Spark 3.2 and MongoDB and MySQL clients. Also there are 2 service containers with MySQL and MongoDB 
servers. Communication between containers with client services and server services is realized across docker internal 
network. User can choose new user's login and password in MySQL and MongoDB in file docker_configs.

## Usage

### 1. Build containers

```shell
bash build.sh (-c|-u|-a)

OPTIONAL ARGUMENTS: 

-c CentOS - build only CentOS container with 2 service containers
-u Ubuntu - build only Ubuntu container with 2 service containers
-a All    - build both containers with CentOS and Ubuntu
```

### 2. Connect to one of containers via docker-compose command

```shell
[sudo] docker-compose exec (belevich_centos|belevich_ubuntu) /bin/bash 
```

### Connect to mysql server from one of containers

```shell
mysql -u {user} -h belevich_mysql -P 3306 [-p{password}]
```

### Connect to mongodb server from one of containers

```shell
mongo --host belevich_mongodb --port=27017 [<{filename}]
```

## Requirements

1. Ubuntu 16.04 or newer


