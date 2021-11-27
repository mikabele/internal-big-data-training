FROM ubuntu:20.04

#tar timezone configs
ENV TIMEZONE=Europe/Minsk
RUN ln -snf /usr/share/zoneinfo/$TIMEZONE /etc/localtime && \
    echo $TIMEZONE > /etc/timezone

#install main utilities
RUN apt-get update && \
    apt-get install -q -y curl && \
    apt-get install -q -y nano

#install python 3.6
RUN apt-get install -q -y python3 && \
    apt-get install -q -y python3-pip && \
#install java
    apt-get install -q -y openjdk-8-jdk && \
#install scala 2.11
    apt-get install -q -y scala

#install spark
RUN curl -O https://archive.apache.org/dist/spark/spark-3.1.1/spark-3.1.1-bin-hadoop3.2.tgz && \
    tar xvf spark-3.1.1-bin-hadoop3.2.tgz && \
    mv spark-3.1.1-bin-hadoop3.2/ /opt/spark

ENV PATH=$PATH:/opt/spark/bin

#install mongo
RUN curl -fsSL https://www.mongodb.org/static/pgp/server-4.4.asc | apt-key add - && \
    echo "deb [ arch=amd64,arm64 ] https://repo.mongodb.org/apt/ubuntu focal/mongodb-org/4.4 multiverse" > /etc/apt/sources.list.d/mongodb-org-4.4.list && \
    apt-get update -y && \
    apt-get install -y mongodb-org

#install mysql 8 client user
RUN apt-get install -q -y mysql-client

CMD [ "/bin/bash" ]