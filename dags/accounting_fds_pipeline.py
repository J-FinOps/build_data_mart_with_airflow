from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator

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
)

def accounting_fds_dag():

    # 1. 인프라 검증 태스크
    check_hdfs_storage = BashOperator(
        task_id='check_hdfs_storage',
        bash_command='airflow connections get HDFS_DEFAULT || true', 
    )

    # 2. PySpark FDS 분산 연산 태스크
    @task(task_id='execute_fds_logic')
    def run_spark_anomaly_detection():
        from pyspark.sql import SparkSession
        
        # 가상 하둡 네임노드 주소를 마스터 스펙으로 지정하여 로컬 클러스터 점화
        spark = SparkSession.builder \
            .appName("AccountingFDSProcessing") \
            .master("local[*]") \
            .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9820") \
            .getOrCreate()
        
        print("Spark Session 성공적으로 연결 완료 - HDFS 스토리지 제어 시작")
        
        # ------------------------------------------------------------------
        # [구현 영역] 대용량 100만 건 데이터 로드 및 멱등성 적재 로직
        # ------------------------------------------------------------------
        input_path = "hdfs://namenode:9820/user/hive/warehouse/raw_accounting"
        output_path = "hdfs://namenode:9820/user/hive/warehouse/fact_accounting"
        
        # 가상 HDFS로부터 CSV 원천 데이터 파일 로드
        df = spark.read.csv(input_path, header=True, inferSchema=True)

        
        spark.stop()

    check_hdfs_storage >> run_spark_anomaly_detection()

accounting_pipeline = accounting_fds_dag()
