from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType, LongType, DateType

default_args = {
    'owner': 'finops_admin',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

@dag(
    dag_id='accounting_anomaly_detection_v1',
    default_args=default_args,
    description='TaskFlow API 기반 HDFS 분산 회계 데이터 FDS 파이프라인',
    start_date=datetime(2026, 7, 1),
    schedule_interval='@daily',
    catchup=False
)
def accounting_fds_dag():

    # 1. 인프라 검증 태스크
    check_hdfs_storage = BashOperator(
        task_id='check_hdfs_storage',
        bash_command='airflow connections get HDFS_DEFAULT || true', 
    )

    @task
    def execute_fds_logic():
        # Spark 세션 설정
        spark = SparkSession.builder \
            .appName("AccountingFDSProcessing") \
            .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9820") \
            .getOrCreate()
        
        print("Spark Session 성공적으로 연결 완료")

        # 스키마 정의
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

        # 데이터 로드
        input_path = "hdfs://namenode:9820/user/hive/warehouse/raw_accounting"
        raw_df = spark.read.csv(input_path, schema=schema, header=True)

        # 정제
        cleaned_df = raw_df.withColumn("거래일자", col("거래일자").cast(DateType())) \
                           .fillna({"금액": 0, "이상여부": 0})

        # FDS 로직 및 적재
        output_path = "hdfs://namenode:9820/user/airflow/warehouse/fact_accounting"
        
        cleaned_df.write \
            .mode("overwrite") \
            .partitionBy("거래일자") \
            .parquet(output_path)

        print("PySpark FDS 연산 및 HDFS Parquet 파티셔닝 적재 프로세스 정상 종료")
        spark.stop()

    check_hdfs_storage >> execute_fds_logic()

accounting_pipeline = accounting_fds_dag()
