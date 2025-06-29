# Base image with Python 3.11 on Debian Bookworm
FROM bitnami/spark:4.0-debian-12




# (Optional) Install common PySpark dependencies
RUN pip install pyspark==4.0.0
RUN pip install delta-spark==4.0.0
RUN pip install jupyterlab 
RUN pip install pandas 
RUN pip install matplotlib 


# Set environment variables
ENV SPARK_HOME=/opt/bitnami/spark
ENV PATH="$SPARK_HOME/bin:$PATH"
ENV PYSPARK_PYTHON=python
