#!/bin/bash
echo "Waiting for HDFS NameNode to leave safe mode..."
until hdfs dfsadmin -safemode get | grep "Safe mode is OFF"; do
  sleep 5
done
echo "HDFS is ready. Creating directories..."
hdfs dfs -mkdir -p /user/airflow/warehouse
hdfs dfs -mkdir -p /user/hive/warehouse
hdfs dfs -chown -R airflow:supergroup /user/airflow
hdfs dfs -chown -R airflow:supergroup /user/hive
