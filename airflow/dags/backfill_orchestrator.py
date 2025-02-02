from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta


@dag(
    default_args={
        "owner": "airflow",
        "start_date": datetime(2024, 1, 1, 0, 0),
        "end_date": datetime(2025, 1, 1, 0, 0),
    },
    schedule="@hourly",  # Hourly schedule for backfill
    catchup=True,
)
def backfill_orchestrator():

    start = EmptyOperator(task_id="start")

    # Déclencher le DAG Mensuel avec TriggerDagRunOperator
    trigger_hourly_dag = TriggerDagRunOperator(
        task_id="trigger_hourly_dag",
        trigger_dag_id="backfill_extract_hourly",  # Nom du DAG Mensuel
        conf={"execution_date": "{{ execution_date }}"},  # Passer la date d'exécution
        wait_for_completion=False,  # Ne pas bloquer le DAG
    )

    # Déclencher le DAG Mensuel avec TriggerDagRunOperator
    trigger_monthly_dag = TriggerDagRunOperator(
        task_id="trigger_monthly_dag",
        trigger_dag_id="backfill_transform_monthly",  # Nom du DAG Mensuel
        conf={"execution_date": "{{ execution_date }}"},  # Passer la date d'exécution
        wait_for_completion=True,  # Bloquer le DAG en attendant
    )

    end = EmptyOperator(task_id="end")

    start >> [trigger_hourly_dag, trigger_monthly_dag] >> end


# Instantiate the DAG
backfill_orchestrator = backfill_orchestrator()
