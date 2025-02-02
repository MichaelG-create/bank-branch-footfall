"""My first DAG script : 2 consecutive BashOperators with prints"""

# in airflow directory :
# export AIRFLOW_HOME=$(pwd)
from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator

import datetime

import pendulum


with DAG(
    dag_id="banking_pipeline",
    description="hourly extract data from an API, then transform it to a parquet file ",
    start_date=pendulum.datetime(2024, 1, 1, 0, 0, tz="UTC"),
    schedule="0 * * * *",  # every minute 0 (so every hour)
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
) as dag:
    run_this_first = BashOperator(
        task_id="run_first_EXTRACT",
        bash_command=f"export PYTHONPATH=$PYTHONPATH:"
        f"/home/michael/ProjetPerso/Banking_Agency_Traffic && "
        f"python3 /home/michael/ProjetPerso/Banking_Agency_Traffic/extract/query_api.py "
        f'"$(date +"%Y-%m-%d %H:%M")"',
    )

    run_this_second = BashOperator(
        task_id="run_this_second_TRANSFORM",
        bash_command="python3 /home/michael/ProjetPerso/Banking_Agency_Traffic/transform/data_pipeline.py",
    )

    run_this_first >> run_this_second

if __name__ == "__main__":
    dag.test()
