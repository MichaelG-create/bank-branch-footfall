from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from datetime import datetime
import subprocess
import logging


@dag(
    default_args={
        "owner": "airflow",
        "start_date": datetime(2024, 1, 1, 0, 0),
        "end_date": datetime(2025, 1, 1, 0, 0),
    },
    schedule="@monthly",  # Monthly schedule for backfill
    catchup=True,
)
def backfill_transform_monthly():

    @task()
    def process_monthly_data(**kwargs):
        logging.info("Running data_pipeline.py...")
        result = subprocess.run(
            [
                "python3",
                "/home/michael/ProjetPerso/Banking_Agency_Traffic/transform/data_pipeline.py",
            ],
            capture_output=True,
            text=True,
        )

        if result.returncode == 0:
            # If the script executed successfully, print its output to the Airflow logs
            print(result.stdout)
        else:
            # If there was an error, print the error
            print(f"Error running script: {result.stderr}")

    start = EmptyOperator(task_id="start")

    # Process monthly data
    process_data = process_monthly_data()

    end = EmptyOperator(task_id="end")

    start >> process_data >> end


# Instantiate the DAG
backfill_transform_monthly = backfill_transform_monthly()
