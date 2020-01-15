"""
Dev: Data Normalization
"""
from __future__ import print_function

import airflow
from airflow import DAG
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.operators.python_operator import PythonOperator,BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator

import os,sys
sys.path.insert(0,os.path.abspath(os.path.dirname(__file__)))

from dnb.header import *
from dnb.utils import *
import dnb.dnb_atlas_match_lib as dnblib

args = {
    'owner': 'Airflow',
    'depends_on_past': False,
    'start_date': datetime.datetime(2019, 12, 27),
}
"""
Create a DAG to execute tasks
"""
dag = DAG(
    dag_id='dnb_data_normalization_dev',
    default_args=args,
    schedule_interval=None,
)

"""
Trigger module
"""
def conditionally_trigger(context, dag_run_obj):
    """
    This function decides whether or not to Trigger the remote DAG
    Here is triggered always, if success
    """
    dag_run_obj.payload = {'message': 'Go'}
    return dag_run_obj

trigger_op = TriggerDagRunOperator(
    task_id='trigger_next_dag',
    trigger_dag_id='dag_dnb_dummy', #test mode
    python_callable=conditionally_trigger,
    trigger_rule = 'none_failed',
    dag=dag,
)

"""
Execution module
"""
exe_op = PythonOperator(
    task_id = 'task_data_normalization',
    python_callable = dnblib.data_normalizaiton,
    dag = dag,
)

exe_op >> trigger_op

