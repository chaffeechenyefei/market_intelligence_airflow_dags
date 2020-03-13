import os,sys
sys.path.insert(0,os.path.abspath(os.path.dirname(__file__)))

import dnb.reason_lib as rslib

print('##one function test')
print('rslib.prod_prepare_data()')
rslib.prod_prepare_data()
#42: Los Angeles
# print('rslib.prod_all_reason_in_one_func()')
# rslib.prod_all_reason_in_one_func(ind_city=42)
print('##Done')