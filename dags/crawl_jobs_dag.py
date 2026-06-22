from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


SOURCES = ['topcv', 'itviec', 'vietnamworks', 'careerviet']

CRAWLER_IMAGE = 'job-crawler:latest'
SPARK_IMAGE = 'job-spark:latest'
STANDARD_IMAGE = 'job-standard:latest'
GOLD_IMAGE = 'job-gold:latest'

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
            mounts=[Mount(source='/home/phongthanh/job-analyst-project/data', target='/app/data', type='bind')],
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
            mounts=[Mount(source='/home/phongthanh/job-analyst-project/data', target='/app/data', type='bind')],
        )
    standard_tasks = {}
    for source in SOURCES:
        standard_tasks[source] = DockerOperator(
            task_id=f'standardize_{source}_to_silver',
            image=STANDARD_IMAGE,
            command=f'python src/transform/transform_bronze_to_silver.py --source {source} --date {{{{ ds }}}}',
            api_version='auto',
            auto_remove='force',
            docker_url='unix://var/run/docker.sock',
            network_mode=NETWORK_MODE,
            mount_tmp_dir=False,
            mounts=[Mount(source='/home/phongthanh/job-analyst-project/data', target='/app/data', type='bind')],
        )

    gold_task = DockerOperator(
        task_id='unify_silver_to_gold',
        image=GOLD_IMAGE,
        command='python src/transform/transform_silver_to_gold.py --date {{ ds }}',
        api_version='auto',
        auto_remove='force',
        docker_url='unix://var/run/docker.sock',
        network_mode=NETWORK_MODE,
        mount_tmp_dir=False,
        mounts=[Mount(source='/home/phongthanh/job-analyst-project/data', target='/app/data', type='bind')],
    )

    publish_postgres_task = DockerOperator(
        task_id='publish_gold_to_postgres',
        image=GOLD_IMAGE,
        command='python src/load/publish_gold_to_postgres.py',
        api_version='auto',
        auto_remove='force',
        docker_url='unix://var/run/docker.sock',
        network_mode=NETWORK_MODE,
        mount_tmp_dir=False,
    )

    for i in range(len(SOURCES)):
        crawl_tasks[SOURCES[i]] >> ingest_tasks[SOURCES[i]] >> standard_tasks[SOURCES[i]] >> gold_task
        
        #Bắt task crawl hiện tại phải đợi task crawl trước đó chạy xong
        if i > 0:
            crawl_tasks[SOURCES[i-1]] >> crawl_tasks[SOURCES[i]]

    gold_task >> publish_postgres_task
