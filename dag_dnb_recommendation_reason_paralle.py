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
from dnb.reason_generator import reason_similar_biz

# sfx = ['', '_right']
# cid = 'duns_number'
# bid = 'atlas_location_uuid'

args = {
    'owner': 'Airflow',
    'depends_on_past': False,
    'start_date': datetime.datetime(2019, 12, 27),
}

"""
Create a DAG to execute tasks
"""
dag = DAG(
    dag_id='dnb_recommendation_reason_paralle',
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

def fake_exe(**op_kwargs):
    print( 'Task:' + op_kwargs['word'] )

def branch_choice(**op_kwargs):
    if op_kwargs['useFLG']:
        return op_kwargs['task'][0]
    else:
        return op_kwargs['task'][1]


prev_city_op_tail = main_op

for ind_city in range(len(citylongname)):
    reason_names = hdargs["reason_col_name"]
    task_city_id = cityabbr[ind_city] + '_begin'
    city_op = PythonOperator(
        task_id = task_city_id,
        python_callable = fake_exe,
        op_kwargs = {
            'word':task_city_id
        },
        dag = dag,
    )
    """
    Each city will run in sequence
    """
    prev_city_op_tail >> city_op

    sub_task_merge_id = cityabbr[ind_city] + '_merge'
    merge_op = PythonOperator(
        task_id = sub_task_merge_id,
        python_callable=fake_exe,
        op_kwargs={
            'word': sub_task_merge_id
        },
        trigger_rule='none_failed',
        dag = dag,
    )

    prev_city_op_tail = merge_op

    for reason_name in reason_names.keys():
        sub_task_branch_id = cityabbr[ind_city] + '_'+ reason_name + '_branching'
        sub_task_exe_id = cityabbr[ind_city] + '_' + reason_name + '_exe'
        sub_task_dummy_id = cityabbr[ind_city] + '_' + reason_name + '_dummy'

        branch_op = BranchPythonOperator(
            task_id= sub_task_branch_id,
            python_callable=branch_choice(),
            op_kwargs={
                'useFLG':reason_names[reason_name]["useFLG"],
                'task':[sub_task_exe_id,sub_task_dummy_id],
            },
            dag=dag,
        )

        exe_op = PythonOperator(
            task_id = sub_task_exe_id,
            python_callable=fake_exe,
            op_kwargs={
                'word': sub_task_exe_id
            },
            dag = dag,
        )

        dummy_op = DummyOperator(
            task_id = sub_task_dummy_id,
            dag=dag,
        )

        city_op >> branch_op >> [exe_op,dummy_op] >> merge_op


prev_city_op_tail >> end_op