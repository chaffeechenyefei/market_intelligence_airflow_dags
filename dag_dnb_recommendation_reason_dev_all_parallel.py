"""
DEV codes
"""
from __future__ import print_function

import time
from builtins import range

import airflow
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator,BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator

import os,sys
sys.path.insert(0,os.path.abspath(os.path.dirname(__file__)))

from dnb.header import *
from dnb.utils import *

from dnb import reason_lib as rslib

args = {
    'owner': 'Airflow',
    'depends_on_past': False,
    'start_date': datetime.datetime(2019, 12, 27),
}

dag_id = 'dnb_recommendation_reason_dev_all_parallel'
dag_cap_lst = [ c[0] for c in dag_id.split('_') if c ]
dag_cap = ''
dag_cap = [dag_cap+c.upper() for c in dag_cap_lst]

"""
Create a DAG to execute tasks
"""
dag = DAG(
    dag_id=dag_id,
    default_args=args,
    schedule_interval=None,
)

main_op = DummyOperator(
    task_id = 'Main_entrance',
    dag= dag,
    )

end_op = DummyOperator(
    task_id = 'End',
    trigger_rule = 'none_failed',
    dag = dag,
)


prev_city_op_tail = main_op



merging_all_op = PythonOperator(
    task_id='merging_all',
    python_callable=rslib.data_merge_for_all_cities,
    trigger_rule = 'all_done',
    dag=dag,
)

for ind_city in range(len(citylongname)):
    if citylongname[ind_city] != 'San Francisco':
        continue
    """
    task of each city
    Each city will run in sequence
    Execution module
    """
    task_exe_id = cityabbr[ind_city] + '_exe'
    exe_op = PythonOperator(
        task_id=task_exe_id,
        provide_context=True,
        python_callable=rslib.prod_all_reason_in_one_func,
        op_kwargs={
            'ind_city': ind_city,
        },
        dag=dag,
    )
    prev_city_op_tail >> exe_op
    prev_city_op_tail = exe_op


prev_city_op_tail >> merging_all_op >> end_op