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
    dag_id='dnb_recommendation_reason_dev',
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

    """
    task of each city
    """
    task_city_id = cityabbr[ind_city] + '_begin'
    #data preparation
    city_op = PythonOperator(
        task_id = task_city_id,
        provide_context=True,
        python_callable = rslib.data_prepare,
        op_kwargs = {
            'ind_city':ind_city,
        },
        dag = dag,
    )
    """
    Each city will run in sequence
    """
    prev_city_op_tail >> city_op

    """
    Name of sub reason result for each city 
    """
    sub_reason_file_names = {}
    for reason_name in reason_names.keys():
        sub_reason_file_names[reason_name] = cityabbr[ind_city] + '_' + reason_name + hdargs['otversion']

    """
    Name of merged reason db for each city
    """
    city_reason_file_name = rsfile[ind_city]

    """
    Merge module: merging all the reason in one db for each city
    """
    sub_task_merge_id = cityabbr[ind_city] + '_merge'
    merge_op = PythonOperator(
        task_id = sub_task_merge_id,
        provide_context=True,
        python_callable= rslib.data_merge_for_city,
        op_kwargs={
            'city_reason_file_name':city_reason_file_name,
            'sub_reason_file_names':sub_reason_file_names,
            'reason_names':reason_names,
            'var_task_space': task_city_id,
        },
        trigger_rule='none_failed',
        dag = dag,
    )

    # prev_tail >> next_head
    prev_city_op_tail = merge_op

    for reason_name in reason_names.keys():
        sub_task_branch_id = cityabbr[ind_city] + '_'+ reason_name + '_branching'
        sub_task_exe_id = cityabbr[ind_city] + '_' + reason_name + '_exe'
        sub_task_skip_read_id = cityabbr[ind_city] + '_' + reason_name + '_skip_read'

        """
        Pick a branch according to hdargs['reason_col_name']['useFLG']
        """
        branch_op = BranchPythonOperator(
            task_id= sub_task_branch_id,
            python_callable=branch_choice,
            op_kwargs={
                'useFLG':reason_names[reason_name]["useFLG"],
                'task':[sub_task_exe_id,sub_task_skip_read_id],
            },
            dag=dag,
        )
        """
        Execution module
        """
        #function for execution, here the function is named as reason_name
        exe_func = getattr(rslib,reason_name)
        #output file name
        sub_reason_file_name = sub_reason_file_names[reason_name]

        exe_op = PythonOperator(
            task_id = sub_task_exe_id,
            provide_context=True,
            python_callable=exe_func,
            op_kwargs={
                'sub_reason_col_name':reason_name,
                'sub_reason_file_name':sub_reason_file_name,
                'var_task_space':task_city_id,
            },
            dag = dag,
        )

        skip_read_op = DummyOperator(
            task_id = sub_task_skip_read_id,
            dag = dag,
        )

        city_op >> branch_op >> [exe_op,skip_read_op] >> merge_op

merging_all_op = PythonOperator(
    task_id='merging_all',
    # provide_context=True,
    python_callable=rslib.data_merge_for_all_cities,
    dag=dag,
)

prev_city_op_tail >> merging_all_op >> end_op