import os
curPath = os.path.abspath(os.path.dirname(__file__))
rootPath = os.path.split(curPath)[0]
sys.path.append(rootPath)

from __future__ import print_function

import time
from builtins import range
from pprint import pprint

import airflow
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator,BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator

from dnb.header import *
from dnb.utils import *
from dnb.reason_generator import *

sfx = ['', '_right']

cid = 'duns_number'
bid = 'atlas_location_uuid'

args = {
    'owner': 'Airflow',
    'depends_on_past': False,
    'start_date': datetime.datetime(2019, 12, 27),
}

"""
Create a DAG to execute tasks
"""
dag = DAG(
    dag_id='dnb_recommendation_reason',
    default_args=args,
    schedule_interval=None,
)


# task_read_data = PythonOperator(
#     task_id = 'task_read_data',
#     provide_context=False,
#     python_callable = ,
#     dag = dag,
#
# )


task_reason_similar_biz_op = BranchPythonOperator(
    task_id='task_reason_similar_biz_branching',
    python_callable='task_gen_reason_similar_biz',
    dag=dag,
)

task_gen_reason_similar_biz_op = PythonOperator(
    task_id = 'task_gen_reason_similar_biz',
    provide_context=True,
    python_callable = reason_similar_biz,
    dag = dag,
)

dummy_gen_reason_similar_biz_op = DummyOperator(
    task_id = 'dummy_gen_reason_similar_biz',
    dag = dag,
)


main_op = DummyOperator(
    task_id = 'Main_entrance',
    dag= dag,
    )

main_op >> task_reason_similar_biz_op >> [ task_gen_reason_similar_biz_op, dummy_gen_reason_similar_biz_op ]