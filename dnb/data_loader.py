import pandas as pd
import numpy as np
import datetime as datetime
import os

pj = os.path.join

class data_process(object):
    def __init__(self, root_path, silence=False):
        self.root_path = root_path
        self.demand_col = {
            'acc_col': 'Organization_Account__c',
            'req_desk': 'Desired_Desks__c',
            'date_col': 'LastModifiedDate',
            'move_in_date': 'Desired_Move_In_Date__c',
        }
        self.inv_col = {
            'bid': 'atlas_location_uuid',
            'date_col': 'report_month',  # 2020/1/1 0:00
            'cap': 'max_reservable_capacity',
            'occ': 'occupancy_rate',  # 0-100
        }
        self.opp_col = {
            'bid': 'atlas_location_uuid',
            'ori_cid': 'AccountId',
            'cid':'account_id',
            'date_col': 'date',
            'status_col': 'status',
            'n_status_col': 'n_status',  # 'Open/Close'
        }
        self.talent_col = {
            'city': 'city',
            'state': 'state',
            'talent_score': 'talent_index',  # 5-1
        }
        self.ls_col = {
            'city':'city',
            'state':'state',
            'lat':'latitude',
            'lng':'longitude',
            'bid':'atlas_location_uuid',
        }
        self.cpstk_col = {
            'uid':'tenant_id',
            'lease_date':'expiration_date',
            'city':'city',
            'bid':'property_id',
            'price':'effective_rent',
            'size':'transaction_size',
            'lng':'longitude',
            'lat':'latitude',
            'submarket':'submarket'
        }

        self.sil = silence
        self.demand_dat = 'salesforce demand signal'
        self.ac_dat = 'salesforce ac_id'
        self.inv_dat = 'inventory data'
        self.op_dat = 'opportunity table'
        self.talent_dat = 'talent table'

    def load_compstak(self,db='compstak',dbname='tetris_mv_tetris_transactions_2016_current.csv'):
        db_path = pj(self.root_path,db)
        uid = self.cpstk_col['uid']
        city = self.cpstk_col['city']
        lease_date = self.cpstk_col['lease_date']
        bid = self.cpstk_col['bid']
        price = self.cpstk_col['price']
        sbm = self.cpstk_col['submarket']
        compstak_db = pd.read_csv(pj(db_path, dbname))[[uid,city,sbm,lease_date,bid,price]]
        print('%d compstak loaded'%len(compstak_db))
        return compstak_db

    def load_submarket_avg_price(self,db='compstak',dbname='tetris_mv_tetris_transactions_2016_current.csv'):
        compstak_db = self.load_compstak(db=db,dbname=dbname)
        compstak_db = compstak_db.dropna(subset=['effective_rent'])
        print('averaging1')
        submarket_avg = compstak_db.groupby(['city', 'submarket'])[['effective_rent']].quantile(
            0.3).reset_index().rename(columns={
            'effective_rent': 'low_effective_rent'
        })
        print('averaging2')
        city_avg = compstak_db.groupby(['city'])[['effective_rent']].quantile(0.3).reset_index().rename(columns={
            'effective_rent': 'low_effective_rent'
        })
        #default value
        print('default value')
        city_avg['submarket'] = 'city_level'

        print('Concat')
        submarket_avg = pd.concat([city_avg, submarket_avg], axis=0, sort=False)
        print('%d average price data generated'%len(submarket_avg))
        return submarket_avg

    def load_compstak_aligned(self,db='compstak',dbname='tetris_mv_tetris_transactions_2016_current.csv'):
        db_path = pj(self.root_path,db)
        uid = self.cpstk_col['uid']
        city = self.cpstk_col['city']
        lease_date = self.cpstk_col['lease_date']
        bid = self.cpstk_col['bid']
        price = self.cpstk_col['price']
        lng = self.cpstk_col['lng']
        lat = self.cpstk_col['lat']
        sz = self.cpstk_col['size']
        sbm = self.cpstk_col['submarket']

        today = datetime.date.today().strftime('%Y-%m-%d')
        compstak_db = pd.read_csv(pj(db_path, dbname))[[uid,city,sbm,lease_date,bid,price,sz,lat,lng]].dropna(subset=[lat,lng])
        compstak_db[sbm] = compstak_db[sbm].fillna('city_level')
        compstak_db[lease_date] = compstak_db[lease_date].fillna(today)
        compstak_db = compstak_db.loc[lambda df: df[lease_date] >= today]
        #logic aligned with Matt
        compstak_db = compstak_db.sort_values([uid,lease_date]).drop_duplicates([uid],keep='first')
        print('%d compstak loaded'%len(compstak_db))
        return compstak_db

    def load_compstak_filter(self, db='compstak', dbname='tetris_mv_tetris_transactions_2016_current.csv'):
        """
        tenant_id,city,property is unique
        expiration_date > current_time
        """
        compstak_dat = self.load_compstak(db=db, dbname=dbname)
        uid = self.cpstk_col['uid']
        city = self.cpstk_col['city']
        lease_date = self.cpstk_col['lease_date']
        bid = self.cpstk_col['bid']
        price = self.cpstk_col['price']

        current_date = datetime.datetime.now()
        current_time = current_date.strftime("%Y-%m-%d")
        cpstk_dat_flt = compstak_dat.loc[compstak_dat[lease_date] > current_time]
        cpstk_dat_dup = cpstk_dat_flt.sort_values([uid, city, bid, lease_date]) \
            .drop_duplicates([uid, city, bid], keep='last')
        print('%d compstak remain after filter' % len(cpstk_dat_dup))
        return cpstk_dat_dup

    def load_cdm_capacity(self, db='inventory', dbname='central_cdm_reservables_20200310.csv'):
        db_path = pj(self.root_path, db, dbname)
        cdm_capacity = pd.read_csv(db_path, error_bad_lines=False)
        #         ['LOCATION_UUID', 'RESERVABLE_UUID', 'RESERVABLE_NAME', 'CAPACITY_DESK',
        #        'TOTAL_PRICE_USD', 'AVAILABILITY', 'MOVE_IN_DATE', 'MOVE_OUT_DATE',
        #        'NOTES', 'AVAILABLE_AT', 'PRODUCT_TYPE']
        cdm_capacity = cdm_capacity[['LOCATION_UUID', 'AVAILABLE_AT', 'CAPACITY_DESK']].rename(
            columns={'LOCATION_UUID': 'atlas_location_uuid',
                     'AVAILABLE_AT': 'available_at', 'CAPACITY_DESK': 'capacity_desk'})
        cdm_capacity = cdm_capacity.dropna().reset_index(drop=True)
        cdm_capacity = cdm_capacity.sort_values(['atlas_location_uuid','available_at','capacity_desk'])\
            .drop_duplicates(['atlas_location_uuid','available_at'],keep='last')
        return cdm_capacity

    def load_inventory(self, db='compstak', dbname='inventory_bom.csv'):
        db_path = pj(self.root_path, db)
        bid = self.inv_col['bid']
        date_col = self.inv_col['date_col']

        inv_dat = pd.read_csv(pj(db_path, dbname))

        inv_dat = inv_dat.sort_values([bid, date_col]) \
            .drop_duplicates([bid], keep='last')

        self.inv_dat = inv_dat
        if not self.sil:
            print('%d atlas loaded' % len(inv_dat))
        return inv_dat

    def load_talent(self, db='talent', dbname='talent_score_v0_01-03-2020.csv'):
        db_path = pj(self.root_path, db)

        city_col = self.talent_col['city']
        talent_score = self.talent_col['talent_score']
        state_col = self.talent_col['state']

        tal_dat = pd.read_csv(pj(db_path, dbname), index_col=0)[[city_col, state_col, talent_score]]

        # check dup
        uniq_num = len(tal_dat.groupby([city_col, state_col]).first())
        assert (uniq_num == len(tal_dat))

        self.talent_dat = tal_dat
        if not self.sil:
            print('%d talent loaded' % len(tal_dat))
        return self.talent_dat

    def load_opportunity(self, db='salesforce', dbname='opportunities.csv', save_dbname='salesforce_pair.csv'):
        db_path = pj(self.root_path, db)
        cid = self.opp_col['cid']
        ori_cid = self.opp_col['ori_cid']
        bid = self.opp_col['bid']
        date_col = self.opp_col['date_col']
        status_col = self.opp_col['status_col']
        n_status_col = self.opp_col['n_status_col']
        kset = set(['closed', 'close', 'closing'])

        op_dat = pd.read_csv(pj(db_path, dbname), header=None,
                             names=['Id', ori_cid, bid, status_col, date_col, 'n2', 'n3', 'n4'])
        op_dat[[date_col]] = op_dat[[date_col]].fillna('1990-01-01')
        op_dat = op_dat.sort_values([ori_cid, bid, date_col]).drop_duplicates([ori_cid, bid], keep='last')
        op_dat[[status_col]] = op_dat[[status_col]].fillna('Closed')
        op_dat[n_status_col] = op_dat[status_col].apply(
            lambda x: 'Closed' if kset & set(x.lower().split(' ')) else 'Open')
        self.op_dat = op_dat.rename(columns={ori_cid: cid})
        #         print(self.op_dat.columns)
        if not self.sil:
            print('%d latest opp loaded' % len(op_dat))
        self.op_dat.to_csv(pj(db_path, save_dbname))
        return self.op_dat

    def load_account(self, db='salesforce', dbname='accounts.csv'):
        db_path = pj(self.root_path, db)
        ac_dat = pd.read_csv(pj(db_path, dbname), error_bad_lines=False, header=None)
        ac_dat = ac_dat[[8, 9]]
        ac_dat = ac_dat.rename(columns={8: 'AccountId', 9: 'Name'})
        self.ac_dat = ac_dat
        if not self.sil:
            print('%d accounts loaded' % len(ac_dat))

    def load_demand(self, db='salesforce', dbname='demand_signals_191110.csv'):
        db_path = pj(self.root_path, db)
        acc_col = self.demand_col['acc_col']
        req_desk = self.demand_col['req_desk']
        date_col = self.demand_col['date_col']
        move_in_date = self.demand_col['move_in_date']  # 2022-01-01T00:00:00Z

        self.demand_dat = pd.read_csv(pj(db_path, dbname))[[acc_col, req_desk, date_col, move_in_date]]
        self.demand_dat[[move_in_date]] = self.demand_dat[[move_in_date]].fillna('1990-01-01')
        self.demand_dat[move_in_date] = self.demand_dat[move_in_date].apply(lambda x: x[:10])  # "%Y-%m-%d"

        if not self.sil:
            print('%d demands signal loaded' % len(self.demand_dat))

    def deduplicate_demand_tb(self, db='salesforce', save_dbname='demand_deduplicate.csv'):
        db_path = pj(self.root_path, db)
        acc_col = self.demand_col['acc_col']
        req_desk = self.demand_col['req_desk']
        date_col = self.demand_col['date_col']
        move_in_date = self.demand_col['move_in_date']

        de_demand_dat = self.demand_dat[[acc_col, req_desk, date_col, move_in_date]]

        def trans_time4sort(x):
            """
            2019-06-19T19:40:55Z => 20190619
            """
            x = str(x)
            x = x.replace('-', '')
            x = x[:8]
            return x

        de_demand_dat['time'] = de_demand_dat[date_col].apply(lambda x: trans_time4sort(x))
        de_demand_dat = de_demand_dat.sort_values([acc_col, 'time']).drop_duplicates([acc_col], keep='last')
        if not self.sil:
            print('%d unique acc demands remains' % len(de_demand_dat))
        if save_dbname:
            de_demand_dat.to_csv(pj(db_path, save_dbname))
        return de_demand_dat

    def load_location_scorecard_msa(self, db='', dbname='location_scorecard_200106.csv',is_wework=False):
        db_path = pj(self.root_path, db)
        bid = self.ls_col['bid']
        state_col = self.ls_col['state']
        city_col = self.ls_col['city']
        wework_col = 'is_wework'

        lsdat = pd.read_csv(pj(db_path, dbname), index_col=0)[[bid, state_col, city_col,wework_col]]
        if is_wework:
            lsdat = lsdat.loc[lsdat[wework_col]==True]
        if not self.sil:
            print('%d location scorecard for msa loaded' % len(lsdat))
        return lsdat

    def load_dnb_city_lst(self, db='reason_table', table='dnb_city_list_200106.csv'):
        db_path = pj(self.root_path, db)
        dnb_city_lst = pd.read_csv(pj(db_path, table), index_col=0)

        citylongname = list(dnb_city_lst['physical_city'].values)
        cityabbr = list(dnb_city_lst['short_name'].values)
        origin_comp_file = list(dnb_city_lst['filename'].values)

        return {
            "citylongname":citylongname,
            "cityabbr":cityabbr,
            "origin_comp_file":origin_comp_file,
        }

