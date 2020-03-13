import os,sys
curPath = os.path.abspath(os.path.dirname(__file__))
rootPath = os.path.split(curPath)[0]
sys.path.append(rootPath)

import pandas as pd
import argparse
from dnb.header import *

sfx = ['','_right']
pj = os.path.join

cid = 'duns_number'
bid = 'atlas_location_uuid'
fid = 'account_id'
fname = 'account_orig_name'
fname_formal = 'account_name'
city = 'physical_city'


def load_salesforce_dnb_match(db='' ,table='relation_dnb_account_0120.csv'):
    db_path = pj(datapath, db)
    sfdnb = pd.read_csv(pj(db_path, table), index_col=0)
    bid = 'atlas_location_uuid'
    cid = 'duns_number'
    fid = 'account_id'
    city = 'physical_city'
    dnb_city = sfdnb.groupby([cid ,city]).first().reset_index()[[cid ,city]].rename(columns={city :'city'})
    print( '%d dnb_city generated' %len(dnb_city))
    dnb_city.to_csv(pj(datapath_mid ,salesforce_dnb_file))
    return sfdnb


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    arg = parser.add_argument
    arg('--dnb_acc', default='')
    arg('--dbname',default='reason_table')

    args = parser.parse_args()

    if args.dnb_acc:
        dnb_acc = args.dnb_acc
    else:
        dnb_acc = salesforce_dnb_match_file
    print('Using dnb_acc:%s'%dnb_acc)

    sfdnb = load_salesforce_dnb_match(db='',table=dnb_acc)

    dedup_sfdnb = sfdnb.drop_duplicates([fid,cid,city], keep='first').reset_index()[[fid,fname,cid,city]]
    # assert(len(dedup_sfdnb.loc[dedup_sfdnb[cid]==74157331]) > 0)
    dedup_sfdnb = dedup_sfdnb.rename(columns={fname:fname_formal})

    print('Duplicate Shrinkage: %1.2f'% (len(dedup_sfdnb)/len(sfdnb)))

    total = len(dedup_sfdnb)

    sfdnb_lst = []
    for ind_city,cur_city_name in enumerate(citylongname):
        """
        Here filter is a must. Because not all the duns_number is valid. Need to check why?
        """
        print('## %s ##'%cur_city_name )
        comp_file = cfile[ind_city]
        comp_dat = pd.read_csv( pj(datapath,comp_file))[[cid,city,'physical_zip_all','msa','latitude','longitude','business_name']]
        comp_dat['state'] = comp_dat['msa'].apply(lambda x: str(x).replace(' ','').split(',')[-1] )
        tmp = dedup_sfdnb.merge(comp_dat, on=[cid, city], suffixes=sfx)
        sfdnb_lst.append( tmp )
        print('%d'%len(tmp))

    sfdnb_lst = pd.concat(sfdnb_lst,axis=0)

    print( '%d of %d covered'% (len(sfdnb_lst),total))

    sfdnb_lst = sfdnb_lst.rename(columns={
        'physical_zip_all':'zip_code',
        city:'city',
        'business_name':'company_name',
        fid:'sfdc_account_id',
    })

    sfdnb_lst = sfdnb_lst.drop_duplicates(['sfdc_account_id', cid, 'city'], keep='first').reset_index()
    print('Second Shrinkage: %d'%len(sfdnb_lst))
    sfdnb_lst.to_csv(pj(datapath_mid,salesforce_dnb_info_file))

    print('Done')



