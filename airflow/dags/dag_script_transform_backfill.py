"""DAG script : Extract CSV files then transform them and store them in parquet"""

# in airflow directory :
# export AIRFLOW_HOME=$(pwd)
from airflow.decorators import dag, task
from airflow.operators.dummy_operator import DummyOperator
import pendulum
import datetime
import os


@dag(
    dag_id="banking_pipeline_transform_backfill",
    description="monthly extract data from an API, then transform it to a parquet file",
    start_date=pendulum.datetime(2024, 2, 1, 0, 0, tz="UTC"),
    end_date=pendulum.datetime(2025, 1, 1, 0, 0, tz="UTC"),  # Stop date
    schedule="0 0 1 * *",  # every hour at minute 0
    catchup=True,
    dagrun_timeout=datetime.timedelta(minutes=60),
)
def banking_pipeline_transform_backfill():

    @task
    def run_second_transform(**kwargs):
        execution_date = kwargs["execution_date"]
        if (
            execution_date.day == 1 and execution_date.hour == 0
        ):  # Check if it's the first of the month so run it
            os.system(
                "python3 /home/michael/ProjetPerso/Banking_Agency_Traffic/transform/data_pipeline.py"
            )

    # Dummy task to set up the flow order
    start = DummyOperator(task_id="start")

    # Setting up dependencies
    start >> run_second_transform()


# Instantiate the DAG
dag_instance = banking_pipeline_transform_backfill()
