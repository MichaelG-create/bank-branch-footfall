"""DAG script : Extract CSV files then transform them and store them in parquet"""

# in airflow directory :
# export AIRFLOW_HOME=$(pwd)
from airflow.decorators import dag, task
from airflow.operators.dummy_operator import DummyOperator
import pendulum
import datetime
import os


@dag(
    dag_id="banking_pipeline_extract_backfill",
    description="hourly extract data from an API, then transform it to a parquet file",
    start_date=pendulum.datetime(2024, 1, 1, 0, 0, tz="UTC"),
    end_date=pendulum.datetime(2024, 12, 31, 23, 0, tz="UTC"),  # Stop date
    schedule="0 * * * *",  # every hour at minute 0
    catchup=True,
    dagrun_timeout=datetime.timedelta(minutes=60),
)
def banking_pipeline_extract_backfill():

    @task
    def run_first_extract(**kwargs):
        # Get the theoretical execution date from the context
        execution_date = kwargs["execution_date"]
        theoretical_time = execution_date.strftime("%Y-%m-%d %H:%M")

        # Use the theoretical time in the bash command
        bash_command = (
            f"export PYTHONPATH=$PYTHONPATH:/home/michael/ProjetPerso/Banking_Agency_Traffic && "
            f"python3 /home/michael/ProjetPerso/Banking_Agency_Traffic/extract/query_api.py "
            f'"{theoretical_time}"'
        )
        os.system(bash_command)

    # Dummy task to set up the flow order
    start = DummyOperator(task_id="start")

    # Setting up dependencies
    start >> run_first_extract()


# Instantiate the DAG
dag_instance = banking_pipeline_extract_backfill()
