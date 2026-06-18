from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


SOURCES = ['topcv', 'topdev', 'itviec', 'vietnamworks']

CRAWLER_IMAGE = 'job-crawler:latest'
SPARK_IMAGE = 'job-spark:latest'

# Network chung để các container có thể giao tiếp với MinIO, Spark, Hive
NETWORK_MODE = 'job-analyst-project_default'

with DAG(
    'daily_job_pipeline',
    default_args=default_args,
    description='Pipeline: Crawl dữ liệu từ các trang tuyển dụng → Ingest từ Landing lên Bronze',
    schedule='0 2 * * *',
    start_date=datetime(2026, 6, 17),
    catchup=False,
    tags=['crawler', 'ingest', 'bronze', 'job-analyst', 'docker'],
) as dag:

    crawl_tasks = {}
    for source in SOURCES:
        crawl_tasks[source] = DockerOperator(
            task_id=f'crawl_{source}',
            image=CRAWLER_IMAGE,
            command=f'python src/extract/{source}.py',
            api_version='auto',
            auto_remove='force',
            docker_url='unix://var/run/docker.sock',
            network_mode=NETWORK_MODE,
            mount_tmp_dir=False,
        )

    ingest_tasks = {}
    for source in SOURCES:
        # Truyền ngày chạy thực tế của Airflow (logical_date) vào script
        ingest_tasks[source] = DockerOperator(
            task_id=f'ingest_{source}_to_bronze',
            image=SPARK_IMAGE,
            command=f'python src/extract/ingest_landing_to_bronze.py --source {source} --date {{{{ ds }}}}',
            api_version='auto',
            auto_remove='force',
            docker_url='unix://var/run/docker.sock',
            network_mode=NETWORK_MODE,
            mount_tmp_dir=False,
        )
    standard_tasks = {}
    for source in SOURCES:
        standard_tasks[source] = DockerOperator(
            task_id=f'standardize_{source}_to_silver',
            image=SPARK_IMAGE,
            command=f'python src/transform/transform_bronze_to_silver.py --source {source} --date {{{{ ds }}}}',
            api_version='auto',
            auto_remove='force',
            docker_url='unix://var/run/docker.sock',
            network_mode=NETWORK_MODE,
            mount_tmp_dir=False,
        )

    # Nối các task lại với nhau: Crawl -> Ingest (Bronze) -> Standardize (Silver)
    for source in SOURCES:
        crawl_tasks[source] >> ingest_tasks[source] >> standard_tasks[source]
