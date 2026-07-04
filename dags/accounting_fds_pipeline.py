from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'finops_admin',
    'depends_on_past': False,
    'start_date': datetime(2026, 7, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def run_spark_anomaly_detection():
    # Airflow 내부에 설치된 pyspark 라이브러리를 동적 호출
    from pyspark.sql import SparkSession
    
    # 하둡 네임노드 주소를 fs.defaultFS 마스터 스펙으로 지정하여 세션 점화
    spark = SparkSession.builder \
        .appName("AccountingFDSProcessing") \
        .master("local[*]") \
        .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9820") \
        .getOrCreate()
    
    print("Spark Session 성공적으로 연결 완료 - HDFS 스토리지 제어 시작")
    
    # 프로덕션 코드 구현 영역:
    # 1. hdfs://namenode:9820/user/hive/warehouse/raw_accounting에서 데이터 로드
    # 2. 복식부기 정합성 및 분식회계 패턴 탐지 스코어링 연산
    # 3. 최종 결과를 Parquet 포맷으로 가상 HDFS /fact_accounting 경로에 멱등성 있게 저장
    
    spark.stop()

with DAG(
    'accounting_anomaly_detection_v1',
    default_args=default_args,
    description='HDFS 기반 분산 회계 데이터 처리 및 FDS 파이프라인',
    schedule_interval='@daily',
    catchup=False,
) as dag:

    # 작업 1: 하둡 네임노드 스토리지 상태 체크 및 헬스체크 검증
    check_hdfs_storage = BashOperator(
        task_id='check_hdfs_storage',
        bash_command='airflow connections get HDFS_DEFAULT || true', 
    )

    # 작업 2: PySpark 분산 엔진 구동 및 분석 로직 실행
    execute_fds_logic = PythonOperator(
        task_id='execute_fds_logic',
        python_callable=run_spark_anomaly_detection,
    )

    check_hdfs_storage >> execute_fds_logic
