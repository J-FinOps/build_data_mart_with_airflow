FROM apache/airflow:2.7.1-python3.9

USER root
# PySpark requires Java 17
RUN apt-get update && apt-get install -y openjdk-17-jre-headless && \
    apt-get clean

# Set JAVA_HOME for Java 17
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH=$PATH:$JAVA_HOME/bin

USER airflow
# Reinstall apache-airflow with other packages to ensure dependency compatibility
RUN pip install --no-cache-dir \
    apache-airflow==2.7.1 \
    pyspark \
    apache-airflow-providers-apache-spark
