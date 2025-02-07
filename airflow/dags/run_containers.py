from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from datetime import datetime

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 2, 1),
    "retries": 1,
}

with DAG("run_containers", default_args=default_args, schedule_interval=None, catchup=False) as dag:

    extract_task = DockerOperator(
        task_id="run_etl_extract",
        image="bank-branch-footfall_etl_extract:latest",
        auto_remove=True,
        command=["python", "extract/extract.py"],
        network_mode="bridge",
    )

    transform_task = DockerOperator(
        task_id="run_etl_transform",
        image="bank-branch-footfall_etl_transform_load:latest",
        auto_remove=True,
        command=["python", "transform_load/transform_load.py"],
        network_mode="bridge",
    )

    web_app_task = DockerOperator(
        task_id="run_web_app",
        image="bank-branch-footfall_web_app:latest",
        auto_remove=True,
        command=["streamlit", "run", "app/app.py"],
        network_mode="bridge",
    )

    extract_task >> transform_task >> web_app_task  # Orchestration des tâches
