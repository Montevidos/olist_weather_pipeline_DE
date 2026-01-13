from airflow.decorators import dag
from airflow.operators.bash import BashOperator
from datetime import datetime

@dag(
    dag_id = "dbt_dag",
    start_date = datetime(2025, 1, 1),
    schedule = "10 10 * * *",
    catchup = False
)
def dbt_run():
    BashOperator(
        task_id = "dbt_run_task",
        bash_command = "cd /opt/airflow/dbt_olist && dbt run"
    )
dbt_run()