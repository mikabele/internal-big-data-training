FROM centos:7

#tar timezone configs
ENV TIMEZONE=Europe/Minsk
RUN ln -snf /usr/share/zoneinfo/$TIMEZONE /etc/localtime && \
    echo $TIMEZONE > /etc/timezone

#install main utilities
RUN yum update -y && \
    yum install -y wget && \
    yum install -y nano

#install python
RUN yum install -y python3 && \
    yum install -y python3-pip && \
#install java
    yum install -y java-1.8.0-openjdk-devel && \
#install scala
    wget http://downloads.lightbend.com/scala/2.11.8/scala-2.11.8.rpm && \
    yum install -y scala-2.11.8.rpm

#install spark
RUN wget http://www-eu.apache.org/dist/spark/spark-3.1.2/spark-3.1.2-bin-hadoop3.2.tgz && \
    tar -xzf spark-3.1.2-bin-hadoop3.2.tgz && \
    mv spark-3.1.2-bin-hadoop3.2/ /opt/spark

ENV PATH=$PATH:/opt/spark/bin

#install mongo
RUN echo $'[mongodb-org-4.4] \n\
name = MongoDB Repository \n\
baseurl = https://repo.mongodb.org/yum/redhat/$releasever/mongodb-org/4.4/x86_64/ \n\
gpgcheck = 1 \n\
enabled = 1 \n\
gpgkey = https://www.mongodb.org/static/pgp/server-4.4.asc' > /etc/yum.repos.d/mongodb-org.repo && \
    yum install -y mongodb-org

#install mysql-client
RUN rpm -Uvh https://repo.mysql.com/mysql80-community-release-el7-3.noarch.rpm && \
    sed -i 's/enabled=1/enabled=0/' /etc/yum.repos.d/mysql-community.repo && \
    yum --enablerepo=mysql80-community install -y mysql-community-server

CMD [ "/bin/bash" ]