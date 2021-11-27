FROM ubuntu:18.04

RUN apt-get update && \
    apt-get install -q -y curl && \
    apt-get install -q -y sudo && \
    apt-get install -q -y gnupg && \
    apt-get install -q -y unzip && \
    apt-get install -q -y nano

RUN apt-get install -q -y openjdk-8-jre

RUN apt-get install -q -y scala && \
    echo "deb https://repo.scala-sbt.org/scalasbt/debian all main" | sudo tee /etc/apt/sources.list.d/sbt.list && \
    echo "deb https://repo.scala-sbt.org/scalasbt/debian /" | sudo tee /etc/apt/sources.list.d/sbt_old.list && \
    curl -sL "https://keyserver.ubuntu.com/pks/lookup?op=get&search=0x2EE0EA64E40A89B84B2DF73499E82A75642AC823" | sudo apt-key add && \
    apt-get update -y && \
    apt-get install -q -y sbt

#install gcloud
RUN echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] http://packages.cloud.google.com/apt cloud-sdk main" | tee -a /etc/apt/sources.list.d/google-cloud-sdk.list && \
    curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | apt-key --keyring /usr/share/keyrings/cloud.google.gpg  add - && \
    apt-get update -y && \
    apt-get install google-cloud-sdk -y

COPY ./gcloud_auth_key_file.json ./gcloud_auth_key_file.json

#RUN ssh-keygen -f ~/.ssh/id_rsa -N ""
#
#RUN gcloud auth activate-service-account test-account@perfect-trilogy-317510.iam.gserviceaccount.com --key-file=./gcloud_auth_key_file.json

## Create non-root user
RUN groupadd --system jenkins --gid 444 && \
    useradd --uid 444 --system --gid jenkins --home-dir /home/snowflake --create-home --shell /sbin/nologin --comment "Docker image user" jenkins && \
    chown -R jenkins:jenkins /home/snowflake && \
    usermod -aG sudo jenkins

# default to being in the user's home directory
WORKDIR /home/snowflake

ENV LC_ALL=C.UTF-8
ENV LANG=C.UTF-8
ENV VERSION 1.1.62
ENV SNOWSQL_DEST /usr/local/bin
ENV SNOWSQL_LOGIN_SHELL /home/snowflake/.bashrc

# grab the installation script
RUN curl -o snowsql-${VERSION}-linux_x86_64.bash http://s3-us-west-2.amazonaws.com/sfc-snowsql-updates/bootstrap/1.1/linux_x86_64/snowsql-${VERSION}-linux_x86_64.bash

# Install the tool
RUN bash snowsql-${VERSION}-linux_x86_64.bash

# Switch to the non-root user
USER jenkins

CMD /bin/bash