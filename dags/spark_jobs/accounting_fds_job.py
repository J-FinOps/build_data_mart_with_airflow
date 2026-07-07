from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType, LongType, DateType

def run_fds_job():
    spark = SparkSession.builder \
        .appName("AccountingFDSProcessingYARN") \
        .getOrCreate()
    
    print("Spark Session 성공적으로 연결 완료 (YARN Client Mode)")

    schema = StructType([
        StructField("거래번호", StringType(), False),
        StructField("거래일자", StringType(), False),
        StructField("계정과목", StringType(), True),
        StructField("거래처", StringType(), True),
        StructField("금액", LongType(), True),
        StructField("적요", StringType(), True),
        StructField("담당자", StringType(), True),
        StructField("승인자", StringType(), True),
        StructField("이상여부", LongType(), True),
        StructField("이상유형", StringType(), True)
    ])

    input_path = "hdfs://namenode:9820/user/hive/warehouse/raw_accounting"
    raw_df = spark.read.csv(input_path, schema=schema, header=True)

    cleaned_df = raw_df.withColumn("거래일자", col("거래일자").cast(DateType())) \
                       .fillna({"금액": 0, "이상여부": 0})

    output_path = "hdfs://namenode:9820/user/airflow/warehouse/fact_accounting"
    
    cleaned_df.write \
        .mode("overwrite") \
        .partitionBy("거래일자") \
        .parquet(output_path)

    print("PySpark FDS 연산 및 HDFS Parquet 파티셔닝 적재 프로세스 정상 종료 (YARN)")
    spark.stop()

if __name__ == "__main__":
    run_fds_job()
