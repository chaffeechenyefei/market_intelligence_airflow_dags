"""
Dev: Training Bash
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
import dnb.dnb_atlas_match_lib as dnblib
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
    dag_id='dnb_prediction_new_account_dev',
    default_args=args,
    schedule_interval=None,
)

prediction_exe = hdargs["dnb_dnn_prediction_exe"]
program_path = hdargs["dnb_dnn_program_path"]

run_root = hdargs["dnb_dnn_cmd"]["run_root"]
model = hdargs["dnb_dnn_cmd"]["model"]
lr = hdargs["dnb_dnn_cmd"]["lr"]
apps = hdargs["apps"]
dbname = dnbdbname

if hdargs["use_additional_feat"]:
    feat_cmd = '--addition'
else:
    feat_cmd = ''

bash_cmd_predict = 'cd %s && python3 -u %s ' \
                    '--run_root %s ' \
                    '--model %s ' \
                    '--lr %1.4f ' \
                    '--apps %s ' \
                    '--dbname %s ' \
                    '--data_path %s ' \
                    '--mode predict --batch-size 1 --airflow --all ' \
                    '%s ' \
                    '--new_account' \
           % (program_path, prediction_exe, run_root, model, lr, apps, dbname, datapath,feat_cmd)
print('bash_cmd_predict: >> %s' % bash_cmd_predict)
exe_op = BashOperator(
    task_id='dnb_prediction',
    bash_command=bash_cmd_predict,
    dag=dag,
)


task_data_id = 'dnb_produce_prediction_pair_new_account'
pair_file = '%s_ww_loc_x_duns_new_account.csv'
data_op = PythonOperator(
    task_id = task_data_id,
    python_callable = dnblib.prod_prediction_pair, #depends on embedding file
    op_kwargs = {
        'save_filename':pair_file,
        'new_account':True,
    },
    dag = dag,
)

data_op >> exe_op







