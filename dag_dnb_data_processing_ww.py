"""
Dev: Match DnB with Atlas
This will match the DnB with the ww locations only. Designed for prediction.
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
from dnb.data_loader import *

args = {
    'owner': 'Airflow',
    'depends_on_past': False,
    'start_date': datetime.datetime(2019, 12, 27),
}
"""
Create a DAG to execute tasks
"""
dag = DAG(
    dag_id='dnb_data_processing_ww_dev',
    default_args=args,
    schedule_interval=None,
)


precision = hdargs["geo_bit"]
dist_thresh = hdargs["dist_thresh"]
# cfile = origin_comp_file
# clfile = cityabbr

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
    trigger_dag_id='dag_dnb_dummy', #test mode
    python_callable=conditionally_trigger,
    trigger_rule = 'none_failed',
    dag=dag,
)


"""
Loading module
"""
task_load = 'task_data_load_ww'
load_op = PythonOperator(
    task_id = task_load,
    provide_context = True,
    python_callable = dnblib.data_load_ww_geohash,
    op_kwargs = {
        'ls_card':hdargs["ls_card"],
        'precision':precision,
    },
    dag = dag,
)

dataloader = data_process(root_path=hdargs["run_root"])
table_name = 'dnb_city_list%s'%apps
dnb_city_file_lst = dataloader.load_dnb_city_lst(db=dnbdbname,table=table_name)

cityabbr = dnb_city_file_lst['cityabbr']
citylongname = dnb_city_file_lst['citylongname']
origin_comp_file = dnb_city_file_lst['origin_comp_file']

cfile = origin_comp_file
clfile = cityabbr

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
        python_callable = dnblib.dnb_atlas_match_ww,
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

    load_op >> exe_op


