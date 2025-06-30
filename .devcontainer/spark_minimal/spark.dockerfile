# Base image with Python 3.11 on Debian Bookworm
FROM bitnami/spark:4.0-debian-12

# Set environment variables
ENV SPARK_HOME=/opt/bitnami/spark
ENV PATH="$SPARK_HOME/bin:$PATH"
ENV PYSPARK_PYTHON=python
