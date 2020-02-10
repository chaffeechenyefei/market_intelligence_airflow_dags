"""
DEV codes
"""
from __future__ import print_function

import datetime
import airflow
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator,BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator

import os,sys
sys.path.insert(0,os.path.abspath(os.path.dirname(__file__)))



args = {
    'owner': 'Airflow',
    'depends_on_past': False,
    'start_date': datetime.datetime(2019, 12, 27),
}

dag_id = 'salesforce_recommendation_reason'

"""
Create a DAG to execute tasks
"""
dag = DAG(
    dag_id=dag_id,
    default_args=args,
    schedule_interval=None,
)


task_get_pair = 'task_get_pair'
op_get_pair = PythonOperator(
    task_id=task_get_pair,
    python_callable=,
    dag=dag,
)

task_get_reason = 'task_get_reason'
op_get_reason = PythonOperator(
    task_id = task_get_reason,
    python_callable=,
    dag = dag,
)

op_get_pair >> op_get_reason