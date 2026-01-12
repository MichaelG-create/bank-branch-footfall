"""DAG script : 2 consecutive BashOperators :
- extracts data at current date
- transform data every first day of the month
(real_time)
"""

import datetime
import os

import pendulum

from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator

project_home = os.getenv("PROJECT_HOME")

with DAG(
    dag_id="banking_production_pipeline",
    description="hourly extract data from an API, then transform it to a parquet file",
    start_date=pendulum.datetime(2024, 1, 1, 0, 0, tz="UTC"),
    schedule="0 * * * *",  # Every hour at minute 0  ← missing comma fixed
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
) as dag:
    run_this_first = BashOperator(
        task_id="run_first_EXTRACT",
        bash_command=(
            f"export PYTHONPATH=$PYTHONPATH:{project_home} && "
            f"python3 {project_home}/extract/extract.py "
            '"$(date +"%Y-%m-%d %H:%M")"'
        ),
    )

    run_this_second = BashOperator(
        task_id="run_this_second_TRANSFORM",
        bash_command=(f"python3 {project_home}/transform_load/transform_load.py"),
    )

    run_this_first >> run_this_second  # pylint: disable=W0104

if __name__ == "__main__":
    dag.test()
