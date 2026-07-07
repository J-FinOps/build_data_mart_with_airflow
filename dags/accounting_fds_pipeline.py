from datetime import datetime, timedelta
from airflow.decorators import dag
from airflow.operators.bash import BashOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

default_args = {
    'owner': 'finops_admin',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

@dag(
    dag_id='accounting_anomaly_detection_v1',
    default_args=default_args,
    description='Hadoop YARN 리소스 매니저 연동형 HDFS 분산 회계 FDS 파이프라인',
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

    # 2. SparkSubmitOperator로 분리된 스크립트 실행 위임 (YARN)
    execute_fds_logic = SparkSubmitOperator(
        task_id='execute_fds_logic',
        application='/opt/airflow/dags/spark_jobs/accounting_fds_job.py', # 마운트된 볼륨 내 경로
        conn_id='spark_default', # Airflow Spark Connection ID
        name='accounting_fds_job_on_yarn',
        verbose=True,
        # 도커 소형 가상환경 자원 절약을 위한 Spark 메모리 슬림 튜닝
        driver_memory='1024m',
        executor_memory='1024m',
        conf={
            'spark.submit.deployMode': 'client', # 로컬 드라이버 실행(컨테이너 기반 제출에 안전)
            'spark.yarn.am.memory': '1024m',
            'spark.yarn.queue': 'default'        # YARN 리소스 큐 설정
        },
        # Spark Submit 시 YARN 및 하둡 환경변수 주입을 위한 옵션 설정
        env_vars={
            'HADOOP_CONF_DIR': '/opt/hadoop/conf',
            'JAVA_HOME': '/usr/lib/jvm/java-11-openjdk-amd64'
        }
    )

    check_hdfs_storage >> execute_fds_logic

accounting_pipeline = accounting_fds_dag()
