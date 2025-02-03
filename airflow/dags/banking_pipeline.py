"""DAG script : 2 consecutive BashOperators :
- extracts data at current date
- transform data avery first day of the month
(real_time)
"""

# in airflow directory :
# export AIRFLOW_HOME=$(pwd)

import datetime

import pendulum

from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="banking_production_pipeline",
    description="hourly extract data from an API, then transform it to a parquet file ",
    start_date=pendulum.datetime(2024, 1, 1, 0, 0, tz="UTC"),
    schedule="0 * * * *",  # every minute 0 (so every hour)
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
) as dag:
    run_this_first = BashOperator(
        task_id="run_first_EXTRACT",
        bash_command="export PYTHONPATH=$PYTHONPATH:"
        "/home/michael/ProjetPerso/Banking_Agency_Traffic && "
        "python3 /home/michael/ProjetPerso/Banking_Agency_Traffic/extract/query_api.py "
        '"$(date +"%Y-%m-%d %H:%M")"',
    )

    run_this_second = BashOperator(
        task_id="run_this_second_TRANSFORM",
        bash_command="python3 /home/michael/ProjetPerso/Banking_Agency_Traffic/transform/"
        "data_pipeline.py",
    )

    run_this_first >> run_this_second  # pylint: disable= W0104

if __name__ == "__main__":
    dag.test()
