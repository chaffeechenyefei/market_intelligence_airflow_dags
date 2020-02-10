import os,sys
sys.path.insert(0,os.path.abspath(os.path.dirname(__file__)))

import salesforce.reason_lib as rslib

print('##one function test')
rslib.prod_all_reason_in_one_func()
print('##Done')