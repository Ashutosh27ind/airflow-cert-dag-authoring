import airflow.utils.dates
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import datetime, timedelta

default_args = {
    "owner": "airflow",
    "start_date": datetime(2023, 1, 1)
}

with DAG(dag_id="cleaning_dag",
         default_args=default_args,
         schedule_interval="@daily", catchup=False) as dag:

        waiting_for_tasks = ExternalTaskSensor(
            task_id='waiting_for_tasks',
            external_dag_id='my_dag',
            external_task_id='storing'
            ,failed_states=['failed', 'skipped'],
            allowed_states=['success']
        )

        cleaning_xcoms = PostgresOperator(
            task_id='cleaning_xcoms',
            sql='sql/CLEANING_XCOMS.sql',
            postgres_conn_id='postgres'
        )

        waiting_for_tasks >> cleaning_xcoms