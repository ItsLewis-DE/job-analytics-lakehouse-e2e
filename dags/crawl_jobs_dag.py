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

with DAG(
    'daily_job_crawlers',
    default_args=default_args,
    description='DAG để chạy các scripts crawler bằng DockerOperator',
    schedule='0 2 * * *', # Chạy vào lúc 2h sáng mỗi ngày
    start_date=datetime(2026, 6, 17),
    catchup=False,
    tags=['crawler', 'job-analyst', 'docker'],
) as dag:

    # Các thông số chung cho DockerOperator
    DOCKER_IMAGE = 'job-crawler:latest'
    NETWORK_MODE = 'job-analyst-project_default' # Tên network mặc định của docker-compose
    
    common_docker_args = {
        'image': DOCKER_IMAGE,
        'api_version': 'auto',
        'auto_remove': 'force',
        'docker_url': 'unix://var/run/docker.sock',
        'network_mode': NETWORK_MODE,
    }

    crawl_topcv = DockerOperator(
        task_id='crawl_topcv',
        command='python src/extract/topcv.py',
        **common_docker_args
    )

    crawl_topdev = DockerOperator(
        task_id='crawl_topdev',
        command='python src/extract/topdev.py',
        **common_docker_args
    )

    crawl_itviec = DockerOperator(
        task_id='crawl_itviec',
        command='python src/extract/itviec.py',
        **common_docker_args
    )

    crawl_vietnamworks = DockerOperator(
        task_id='crawl_vietnamworks',
        command='python src/extract/vietnamworks.py',
        **common_docker_args
    )

    # Chạy song song cả 4 crawlers
    [crawl_topcv, crawl_topdev, crawl_itviec, crawl_vietnamworks]
