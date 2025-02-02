from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from datetime import datetime
import os


@dag(
    default_args={
        "owner": "airflow",
        "start_date": datetime(2024, 1, 1, 0, 0),
        "end_date": datetime(2025, 1, 1, 0, 0),
    },
    schedule=None,  # Hourly schedule for backfill
    # schedule="@hourly",  # Hourly schedule for backfill
    catchup=True,
)
def backfill_extract_hourly():

    @task()
    def process_hourly_data(**kwargs):
        # Get the theoretical execution date from the context
        conf = kwargs.get("dag_run").conf or {}  # Récupérer la conf
        execution_date = execution_date = conf.get("execution_date", "No execution_date provided")
        theoretical_time = execution_date.strftime("%Y-%m-%d %H:%M")

        # Use the theoretical time in the bash command
        bash_command = (
            f"export PYTHONPATH=$PYTHONPATH:/home/michael/ProjetPerso/Banking_Agency_Traffic && "
            f"python3 /home/michael/ProjetPerso/Banking_Agency_Traffic/extract/query_api.py "
            f'"{theoretical_time}"'
        )
        os.system(bash_command)

    start = EmptyOperator(task_id="start")

    # Process hourly data
    hourly_task = process_hourly_data()

    end = EmptyOperator(task_id="end")

    start >> hourly_task >> end


# Instantiate the DAG
backfill_extract_hourly = backfill_extract_hourly()
