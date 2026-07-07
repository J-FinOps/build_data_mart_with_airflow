#!/bin/bash
echo "Waiting for HDFS NameNode to leave safe mode..."
until hdfs dfsadmin -safemode get | grep "Safe mode is OFF"; do
  sleep 5
done
echo "HDFS is ready. Creating directories..."
hdfs dfs -mkdir -p /user/airflow/warehouse
hdfs dfs -mkdir -p /user/hive/warehouse
hdfs dfs -mkdir -p /user/hive/warehouse/raw_accounting

if [ -f /transactions_100k.csv ]; then
  echo "Uploading transactions_100k.csv to HDFS..."
  hdfs dfs -put -f /transactions_100k.csv /user/hive/warehouse/raw_accounting/transactions_100k.csv
fi

hdfs dfs -chown -R airflow:supergroup /user/airflow
hdfs dfs -chown -R airflow:supergroup /user/hive
