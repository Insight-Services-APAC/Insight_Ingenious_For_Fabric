# Base image with Python 3.11 on Debian Bookworm
FROM mcr.microsoft.com/devcontainers/python:3.11-bookworm

# Install OpenJDK 17
RUN apt-get update && export DEBIAN_FRONTEND=noninteractive \
    && apt-get -y install --no-install-recommends \
    openjdk-17-jre \
    && apt-get autoremove -y && apt-get clean -y && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

# Install Spark 3.5.0
RUN cd /tmp && \
    wget https://archive.apache.org/dist/spark/spark-3.5.0/spark-3.5.0-bin-hadoop3.tgz && \
    cd / && \
    tar -xzf /tmp/spark-3.5.0-bin-hadoop3.tgz && \
    mv /spark-3.5.0-bin-hadoop3 /spark && \
    rm -rf /tmp/spark-3.5.0-bin-hadoop3.tgz

ENV SPARK_HOME=/spark
ENV PATH="$SPARK_HOME/bin:$PATH"
