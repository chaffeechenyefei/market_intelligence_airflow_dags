"""
Dev: Normalization bash
Param are given
"""
from __future__ import print_function

import airflow
from airflow import DAG
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.operators.python_operator import PythonOperator,BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator

import os,sys
sys.path.insert(0,os.path.abspath(os.path.dirname(__file__)))

from dnb.header import *
import datetime

args = {
    'owner': 'Airflow',
    'depends_on_past': False,
    'start_date': datetime.datetime(2019, 12, 27),
}
"""
Create a DAG to execute tasks
"""
dag = DAG(
    dag_id='dnb_normalization_with_param',
    default_args=args,
    schedule_interval=None,
)

program_exe = hdargs["dnb_data_normalizaiton_with_param_exe"]
program_path = hdargs["dnb_dnn_normalization_path"]

run_root = hdargs["run_root"]
ls_card = hdargs["ls_card"]
apps = hdargs["apps"]
app_date = apps.replace('.csv','')
dbname = dnbdbname


bash_cmd_predict = 'cd %s && python3 -u %s ' \
                   '--run_root %s ' \
                   '--ls_card %s ' \
                   '--app_date %s ' \
                   '--dbname %s '\
                   %(program_path,program_exe,run_root,ls_card,app_date,dbname)
# print('bash_cmd_predict: >> %s' % bash_cmd_predict)
exe_op = BashOperator(
    task_id='dnb_normalization',
    bash_command=bash_cmd_predict,
    dag=dag,
)







