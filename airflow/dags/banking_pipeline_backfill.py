"""DAG script : Extract CSV files then transform them and store them in parquet"""

# in airflow directory :
# export AIRFLOW_HOME=$(pwd)
import datetime
import os

import pendulum

from airflow.decorators import task
from airflow.models.dag import DAG

with DAG(
    dag_id="banking_pipeline_backfill",
    description="hourly extract data from an API, then transform it to a parquet file",
    start_date=pendulum.datetime(2024, 1, 1, 0, 0, tz="UTC"),
    end_date=pendulum.datetime(2025, 1, 1, 0, 0, tz="UTC"),  # Stop date
    schedule="0 * * * *",  # every hour at minute 0
    catchup=True,
    dagrun_timeout=datetime.timedelta(minutes=60),
) as dag:

    @task
    def run_first_extract(**kwargs):
        """run extract task via a query to the local or distant API
        and save a CSV for every hour for all agencies"""
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

    @task
    def run_second_transform(**kwargs):
        """run transform task collecting CSVs"""
        execution_date = kwargs["execution_date"]
        if (
            execution_date.day == 1 and execution_date.hour == 0
        ):  # Check if it's the first of the month so run it
            os.system(
                "python3 /home/michael/ProjetPerso/Banking_Agency_Traffic/transform/"
                "data_pipeline.py"
            )

    # Setting up dependencies
    run_first_extract() >> run_second_transform()  # pylint: disable= W0106


if __name__ == "__main__":
    dag.test()
