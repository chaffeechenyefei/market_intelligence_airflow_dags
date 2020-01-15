"""
Dev: Match DnB with Atlas
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
    dag_id='dnb_data_processing_dev',
    default_args=args,
    schedule_interval=None,
)


precision = hdargs["geo_bit"]
dist_thresh = hdargs["dist_thresh"]
cfile = origin_comp_file
clfile = cityabbr

apps = hdargs["apps"]

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
    trigger_dag_id='dnb_data_normalization_dev', #test mode
    python_callable=conditionally_trigger,
    trigger_rule = 'none_failed',
    dag=dag,
)


"""
Loading module
"""
task_load = 'task_data_load'
load_op = PythonOperator(
    task_id = task_load,
    provide_context = True,
    python_callable = dnblib.data_load,
    op_kwargs = {
        'ls_card':hdargs["ls_card"]
    },
    dag = dag,
)

"""
Execution module
"""
for ind_city in range(len(cfile)):
    cur_city_abbr_name = cityabbr[ind_city]
    cur_city_name = citylongname[ind_city]

    sub_task_id = 'task_%s'%cur_city_abbr_name
    clfile_name =  cur_city_abbr_name + apps

    exe_op = PythonOperator(
        task_id = sub_task_id,
        provide_context=True,
        python_callable = dnblib.dnb_atlas_match,
        op_kwargs = {
            'cfile':cfile[ind_city],
            'lfile':hdargs["ls_card"],
            'clfile':clfile_name,
            'precision':precision,
            'dist_thresh':dist_thresh,
            'var_task_space':task_load,
        },
        dag = dag,
    )

    load_op >> exe_op >> trigger_op


