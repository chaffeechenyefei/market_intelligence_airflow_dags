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
fname = 'account_name'
city = 'physical_city'


def load_salesforce_dnb_match(self ,db='' ,table='relation_dnb_account_0120.csv'):
    db_path = pj(self.root_path, db)
    sfdnb = pd.read_csv(pj(db_path, table), index_col=0)
    bid = 'atlas_location_uuid'
    cid = 'duns_number'
    fid = 'account_id'
    city = 'physical_city'
    dnb_city = sfdnb.groupby([cid ,city]).first().reset_index()[[cid ,city]].rename(columns={city :'city'})
    print( '%d dnb_city generated' %len(dnb_city))
    dnb_city.to_csv(pj(datapath_mid ,'salesforce_comp_city_from_opp.csv'))
    return sfdnb


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    arg = parser.add_argument
    arg('--dnb_acc', default='relation_dnb_account_0120.csv')
    arg('--dbname',default='reason_table')

    args = parser.parse_args()

    sfdnb = load_salesforce_dnb_match(db='',table=args.dnb_acc)

    dedup_sfdnb = sfdnb.drop_duplicates([fid,cid,city], keep='first').reset_index()[[fid,fname,cid,city]]

    print('Shrinkage: %f'%len(dedup_sfdnb)/len(sfdnb))

    total = len(dedup_sfdnb)

    for ind_city,cur_city_name in enumerate(citylongname):
        print('## %s ##'%cur_city_name )
        comp_file = cfile[ind_city]
        comp_dat = pd.read_csv( pj(datapath,comp_file))[[cid,city,'physical_zip_all','msa','latitude','longitude','business_name']]
        comp_dat['state'] = comp_dat['msa'].apply(lambda x: str(x).replace(' ','').split(',')[-1] )
        dedup_sfdnb = dedup_sfdnb.merge(comp_dat,on=[cid,city],how='left')

    print( '%d of %d covered'%len(dedup_sfdnb.loc[dedup_sfdnb['business_name'].notnull()]),total)

    dedup_sfdnb = dedup_sfdnb.rename(columns={
        'physical_zip_all':'zip_code',
        city:'city',
        'business_name':'company_name',
    })

    dedup_sfdnb.to_csv(pj(datapath_mid,'salesforce_acc_duns_info.csv'))



