"""My first DAG script : 2 consecutive BashOperators with prints"""

from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator

import datetime

import pendulum


with DAG(
    dag_id="pipeline",
    description="hourly extract data from an API, then transform it to a parquet file ",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    schedule="0 * * * *",  # every minute 0 (so every hour)
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
) as dag:
    run_this_first = BashOperator(
        task_id="run_first_EXTRACT",
        bash_command="python3 ~/ProjetPerso/Banking_Agency_Traffic/airflow/dags/test.py",
    )

    run_this_second = BashOperator(
        task_id="run_this_second_TRANSFORM",
        bash_command="python3 ~/ProjetPerso/Banking_Agency_Traffic/airflow/dags/test.py",
    )

    run_this_first >> run_this_second

if __name__ == "__main__":
    dag.test()
