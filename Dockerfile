FROM python:3.9

ENV JAVA_HOME="/usr/lib/jvm/java-11-openjdk-amd64"

ENV SPARK_HOME="/opt/spark"
ENV SPARK_VERSION="3.0.0"
ENV HADOOP_VERSION="2.7"
ENV PATH="$SPARK_HOME/bin:$PATH"

ENV PYSPARK_PYTHON=python
ENV PATH="$SPARK_HOME/python:$PATH"

WORKDIR /opt

# Java
RUN echo "deb http://ftp.us.debian.org/debian sid main" >> /etc/apt/sources.list
RUN apt-get update && apt-get -y install gcc-8-base openjdk-11-jdk && apt-get -y autoremove

# Spark
ADD "https://archive.apache.org/dist/spark/spark-$SPARK_VERSION/spark-$SPARK_VERSION-bin-hadoop$HADOOP_VERSION.tgz" .
RUN tar -xzf spark*.tgz && rm -f spark*.tgz && mv spark* spark

CMD tail -f /dev/null
