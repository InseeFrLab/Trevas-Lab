ARG java_image_tag=11-jre-slim
FROM openjdk:${java_image_tag}

ARG spark_version=3.0.1
ARG spark_release=3.0.1-bin-hadoop3.2
#ARG spark_version=3.0.1-bin-without-hadoop
ARG hadoop_version=3.2.0
ARG spark_uid=185

#ENV HADOOP_HOME="/opt/hadoop"
ENV SPARK_HOME="/opt/spark"

RUN set -ex && \
    sed -i 's/http:\/\/deb.\(.*\)/https:\/\/deb.\1/g' /etc/apt/sources.list && \
    apt-get update && \
    ln -s /lib /lib64 && \
    apt install -y wget bash tini libc6 libpam-modules krb5-user libnss3 && \
    rm /bin/sh && \
    ln -sv /bin/bash /bin/sh && \
    echo "auth required pam_wheel.so use_uid" >> /etc/pam.d/su && \
    chgrp root /etc/passwd && chmod ug+rw /etc/passwd && \
    rm -rf /var/cache/apt/* /var/lib/apt/lists/*

RUN mkdir -p $SPARK_HOME && wget -q -O- -i https://apache.uib.no/spark/spark-${spark_version}/spark-${spark_release}.tgz \
  | tar xzv -C $SPARK_HOME --strip-components=1

# RUN wget -O- https://apache.uib.no/hadoop/common/hadoop-${hadoop_version}/hadoop-${hadoop_version}.tar.gz
#  | tar xzv -C /opt

RUN wget -q https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${hadoop_version}/hadoop-aws-${hadoop_version}.jar \
      -P $SPARK_HOME/jars
RUN wget -q https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.11.375/aws-java-sdk-bundle-1.11.375.jar \
      -P $SPARK_HOME/jars

COPY entrypoint.sh /opt/

WORKDIR /opt/spark/work-dir
RUN chmod g+w /opt/spark/work-dir

ENTRYPOINT [ "/opt/entrypoint.sh" ]

# Specify the User that the actual main process will run as
USER ${spark_uid}
