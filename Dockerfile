FROM apache/airflow:2.7.1-python3.9

USER root
# PySpark with Java 11
RUN apt-get update && apt-get install -y openjdk-11-jre-headless && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME for Java 11
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV PATH=$PATH:$JAVA_HOME/bin

USER airflow
# Install dependencies with constraints to match Airflow 2.7.1 and Python 3.9
RUN pip install --no-cache-dir \
    "pyspark==3.4.1" \
    "apache-airflow-providers-apache-spark" \
    "apache-airflow-providers-postgres" \
    "psycopg2-binary" \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.7.1/constraints-3.9.txt"
