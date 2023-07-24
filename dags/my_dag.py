from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.decorators import task, dag
from airflow.operators.subdag import SubDagOperator
from airflow.utils.task_group import TaskGroup
from airflow.operators.dummy import DummyOperator
from airflow.sensors.date_time import DateTimeSensor

from datetime import datetime, timedelta
from typing import Dict
from groups.process_tasks import process_tasks
import time

partners = {
    "partner_snowflake":
        {
            "name": "snowflake",
            "path": "/partners/snowflake",
            "priority": 2
        },
    "partner_netflix":
        {
            "name": "netflix",
            "path": "/partners/netflix",
            "priority": 3
        },
    "partner_astronomer":
        {
            "name": "astronomer",
            "path": "/partners/astronomer",
            "priority": 1
        },
}

default_args = {
    "start_date" : datetime(2023, 5, 5)
}

#def _choosing_partner_based_on_day(execution_date):
#    day = execution_date.day_of_week
#    if (day == 1):
#        return 'extract_partner_snowflake'
#    if (day == 3):
#        return 'extract_partner_netflix'
#    if (day == 5):
#        return 'extract_partner_astronomer'
#    return 'stop'


@dag(description="DAG in charge of processing customer data",
        default_args = default_args ,schedule_interval="@daily",
        dagrun_timeout=timedelta(minutes=10),
        tags=["Data_Science", "customers"], catchup=False)

def my_dag():

    start = DummyOperator(task_id="start", trigger_rule='dummy', pool='default_pool', execution_timeout=timedelta(minutes=10))

    #choosing_partner_based_on_day = BranchPythonOperator(
    #    task_id="choosing_partner_based_on_day",
    #    python_callable=_choosing_partner_based_on_day
    #)
    #stop = DummyOperator(task_id="stop")

    storing = DummyOperator(task_id='storing', trigger_rule='none_failed_or_skipped')

    #choosing_partner_based_on_day >> stop
    for partner, details in partners.items():
        @task.python(task_id=f"extract_{partner}",
                     priority_weight=details['priority'], do_xcom_push=False, pool='partner_pool', multiple_outputs=True)

        def extract(partner_name, partner_path):
            time.sleep(3)
            return {"partner_name": partner_name, "partner_path": partner_path}
        extracted_values = extract(details['name'], details['path'])
        start >> extracted_values
        process_tasks(extracted_values) >> storing

dag=my_dag()
