FROM ubuntu:20.04

#tar timezone configs
ENV TIMEZONE=Europe/Minsk
RUN ln -snf /usr/share/zoneinfo/$TIMEZONE /etc/localtime && \
    echo $TIMEZONE > /etc/timezone

#Hadoop

USER root
# open port 22
EXPOSE 22

RUN apt-get update -y && \
    apt-get install -y python python3 && \
    apt-get install -q -y python3-pip && \
    apt-get install -y openjdk-8-jdk && \
    apt-get install -y openssh-server && \
    apt-get install -y nano && \
    apt-get install -y sudo && \
    apt-get install -y git


# install hadoop
RUN wget https://archive.apache.org/dist/hadoop/core/hadoop-3.2.0/hadoop-3.2.0.tar.gz -P /home/hadoop/ && \
    tar -xzf /home/hadoop/hadoop-3.2.0.tar.gz -C /home/hadoop/ && \
    mv /home/hadoop/hadoop-3.2.0 /home/hadoop/hadoop && \
    rm -rf /home/hadoop/hadoop-3.2.0*

#install scala
RUN mkdir /usr/share/scala && \
    wget https://downloads.lightbend.com/scala/2.12.8/scala-2.12.8.tgz -P /tmp/ && \
    tar -xzf /tmp/scala-2.12.8.tgz -C /tmp/ && \
    mv /tmp/scala-2.12.8/* /usr/share/scala/ && \
    rm -rf /tmp/scala-2.12.8 /tmp/scala-2.12.8.tgz && \
    cp /usr/share/scala/bin/* /usr/bin/

#install spark
RUN wget https://archive.apache.org/dist/spark/spark-2.4.0/spark-2.4.0-bin-without-hadoop.tgz -P /home/hadoop/ && \
    tar -xzf /home/hadoop/spark-2.4.0-bin-without-hadoop.tgz -C /home/hadoop/ && \
    mv /home/hadoop/spark-2.4.0-bin-without-hadoop /home/hadoop/spark && \
    rm /home/hadoop/spark-2.4.0-bin-without-hadoop.tgz

# install hive and postgressql jdbc
RUN apt-get install -y libpostgresql-jdbc-java && \
    wget https://archive.apache.org/dist/hive/hive-2.3.4/apache-hive-2.3.4-bin.tar.gz -P /home/hadoop/ && \
    tar -xzf /home/hadoop/apache-hive-2.3.4-bin.tar.gz -C /home/hadoop/ && \
    mv /home/hadoop/apache-hive-2.3.4-bin /home/hadoop/hive && \
    rm -rf /home/hadoop/apache-hive-2.3.4*

# add hadoop user
RUN useradd -m -s /bin/bash hadoop -p hadoop && \
    usermod -aG sudo hadoop && \
    passwd -d hadoop

# set pubkey authentication
RUN echo "PubkeyAuthentication yes" >> /etc/ssh/ssh_config && \
    mkdir -p /home/hadoop/.ssh && \
    echo "PubkeyAcceptedKeyTypes +ssh-dss" >> /home/hadoop/.ssh/config && \
    echo "PasswordAuthentication no" >> /home/hadoop/.ssh/config && \
    echo "MichaelBelevich" | ssh-keygen -t rsa -P "" -f /home/hadoop/.ssh/id_rsa && \
    chmod 400 /home/hadoop/.ssh/id_rsa && \
    chmod 400 /home/hadoop/.ssh/id_rsa.pub && \
    cat /home/hadoop/.ssh/id_rsa.pub >> /home/hadoop/.ssh/authorized_keys && \
    chown hadoop -R /home/hadoop/.ssh

# set environment variables
ENV JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
ENV HADOOP_HOME=/home/hadoop/hadoop
ENV HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
ENV HADOOP_MAPRED_HOME=$HADOOP_HOME
ENV HADOOP_COMMON_HOME=$HADOOP_HOME
ENV HADOOP_HDFS_HOME=$HADOOP_HOME
ENV HADOOP_COMMON_LIB_NATIVE_DIR=$HADOOP_HOME/lib/native
ENV YARN_HOME=$HADOOP_HOME
ENV PATH=$JAVA_HOME/bin:$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$PATH


# create folders for nodes
RUN mkdir -p /home/hadoop/data/nameNode && \
    mkdir -p /home/hadoop/data/dataNode && \
    mkdir -p /home/hadoop/data/nameNodeSecondary && \
    mkdir -p /home/hadoop/data/tmp && \
    mkdir -p /home/hadoop/hadoop/logs

ADD configs/hadoop-env.sh $HADOOP_HOME/etc/hadoop/hadoop-env.sh
ADD configs/core-site.xml $HADOOP_HOME/etc/hadoop/core-site.xml
ADD configs/hdfs-site.xml $HADOOP_HOME/etc/hadoop/hdfs-site.xml
ADD configs/mapred-site.xml $HADOOP_HOME/etc/hadoop/mapred-site.xml
ADD configs/yarn-site.xml $HADOOP_HOME/etc/hadoop/yarn-site.xml
ADD configs/workers $HADOOP_HOME/etc/hadoop/workers


# permissions
RUN chown hadoop -R /home/hadoop/data && \
    chown hadoop -R /home/hadoop/hadoop

#Spark

RUN mkdir /home/hadoop/spark/logs && \
    chown hadoop -R /home/hadoop/spark/logs

# set environment variables
ENV SCALA_HOME=/usr/share/scala
ENV SPARK_HOME=/home/hadoop/spark
ENV SPARK_LOG_DIR=/home/hadoop/spark/logs
# ENV SPARK_DIST_CLASSPATH $(hadoop classpath) does not work
RUN export SPARK_DIST_CLASSPATH=$(hadoop classpath)
ENV PATH=$SCALA_HOME/bin:$SPARK_HOME/sbin:$PATH

RUN mv $SPARK_HOME/conf/spark-env.sh.template $SPARK_HOME/conf/spark-env.sh && \
    echo "export SPARK_DIST_CLASSPATH=$(hadoop classpath)" >> $SPARK_HOME/conf/spark-env.sh && \
    echo "export SPARK_LOG_DIR=/home/hadoop/spark/logs" >> $SPARK_HOME/conf/spark-env.sh && \
    mv $SPARK_HOME/conf/spark-defaults.conf.template $SPARK_HOME/conf/spark-defaults.conf && \
    echo "spark.eventLog.dir file:/home/hadoop/spark/logs" >> $SPARK_HOME/conf/spark-defaults.conf && \
    echo "spark.history.fs.logDirectory file:/home/hadoop/spark/logs" >> $SPARK_HOME/conf/spark-defaults.conf

ADD configs/workers $SPARK_HOME/conf/slaves

RUN chown hadoop -R /home/hadoop/spark

#Hive


# set environment variables
ENV HIVE_HOME=/home/hadoop/hive
ENV PATH=$HIVE_HOME/bin:$PATH

ADD configs/hive-site.xml $HIVE_HOME/conf/hive-site.xml

RUN cp /usr/share/java/postgresql-jdbc4.jar $HIVE_HOME/lib/ && \
    echo "export HADOOP_HOME=/home/hadoop/hadoop" >> $HIVE_HOME/bin/hive-config.sh && \
    chown hadoop -R /home/hadoop/hive
    #export LANGUAGE=en_US.UTF-8

CMD service ssh start && bash