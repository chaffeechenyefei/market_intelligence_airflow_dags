import os,sys
sys.path.insert(0,os.path.abspath(os.path.dirname(__file__)))

import dnb.reason_lib as rslib

print('##one function test')
# print('rslib.prod_prepare_data()')
# rslib.prod_prepare_data()
#42: Los Angeles
for ind_city in [2,15,30,38,44,61,67,78]:
    print('rslib.prod_all_reason_in_one_func()')
    rslib.prod_all_reason_in_one_func(ind_city=ind_city)

rslib.data_merge_for_all_cities()
print('##Done')