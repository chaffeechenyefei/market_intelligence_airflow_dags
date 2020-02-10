from salesforce.header import *
from salesforce.utils import salesforce_pair
from dnb.data_loader import data_process
import pandas as pd
import json
import sys

sfx = ['','_right']

def set_xcom_var(ti, key, value):
    ti.xcom_push(key=key, value=value)


def get_xcom_var(ti, task_id, key):
    return ti.xcom_pull(task_ids=task_id, key=key)


def generate_pairs():
    print('##generating pairs for prediction')
    opp_file = salesforce_file
    lscard_file = hdargs["ls_card"]
    save_pos_pair_name = pj(datapath_mid,mid_salesforce_pos_file)
    save_opp_x_atlas_name = pj(datapath_mid,mid_salesforce_opp_x_atlas_file)

    prgt = salesforce_pair(datapath=datapath,opp_file=opp_file,lscard_file=lscard_file)
    prgt.generate(save_pos_pair_name=save_pos_pair_name,save_opp_x_atlas_name=save_opp_x_atlas_name)
    print('##Done')


def merge_col_into_json(row,src_cols=[],jsKey='reasons'):
    n_reasons = {}
    for src_col in src_cols:
        rs = str(row[src_col]) if row[src_col] else None
        if rs and rs!='':
            dct_rs = json.loads(rs)
            #insert into dict
            for k,v in dct_rs.items():
                w = v if isinstance(v,list) else [v]
                w = [ele for ele in w if ele != '']
                if len(w) == 0:
                    continue
                if k not in n_reasons.keys():
                    n_reasons[k] = w
                else:
                    n_reasons[k] = n_reasons[k]+w

    if n_reasons:
        return json.dumps({jsKey:n_reasons})
    else:
        return ''

def prod_all_reason_in_one_func():
    similairty_file = pj(datapath_mid,mid_salesforce_similairty_file)
    if os.path.isfile(similairty_file):
        sspd = pd.read_csv(similairty_file,index_col=0)
        print('##Reading similarity file: %d loaded'%len(sspd))
    else:
        print('##No similarity file is found. Error.')
        return

    #reason_file
    bid = hdargs["bid"]
    fid = hdargs["fid"]
    kwargs = dict(
        datapath = datapath,
        reason_support_hot_loctation = reason_support_hot_location,
        reason_support_item2item = reason_support_item2item,
        inventory_file=inventory_file,
        compstak_file = compstak_file,
        demand_file = demand_file,
        salesforce_file = salesforce_file,
        salesforce_pos_file = pj(datapath_mid,mid_salesforce_pos_file),
        salesforce_opp_x_atlas_file = pj(datapath_mid,mid_salesforce_opp_x_atlas_file),
        location_scorecard_file = hdargs["ls_card"],
        fid = fid,
        bid = bid,
    )

    print('##updating reason file for each reason')
    reason_file_name_lst = {}
    for reason_col_name,reason_param in hdargs["reason_col_name"].items():
        reason_file_name = pj(datapath_mid, reason_col_name + '.csv')#names of file to be saved for each reason
        reason_file_name_lst[reason_col_name] = reason_file_name
        jsKey = reason_param["rsKey"]
        cacheFLG = reason_param["cache"]
        useFLG = reason_param["useFLG"]
        if useFLG:
            print('##updating reason:%s'%reason_col_name)
            exe_func = locals()[reason_col_name]
            reason_dat = exe_func(sspd= sspd, jsKey=jsKey,**kwargs)
            reason_dat.to_csv(reason_col_name)
        else:
            print('##skipped updating reason:%s because useFLG'%reason_col_name)

    print('##merging them into one data')
    reason_exist = []
    for reason_col_name,reason_param in hdargs["reason_col_name"].items():
        cacheFLG = reason_param["cache"]
        reason_file_name = reason_file_name_lst[reason_col_name]
        if os.path.isfile(reason_col_name) and cacheFLG:
            reason_dat = pd.read_csv(reason_file_name,index_col=0)
            if reason_dat.empty:
                print('==> reason: %s skipped because file: %s is empty'% (reason_col_name,reason_file_name))
            else:
                reason_exist.append(reason_col_name)
                join_key = list(set([bid,fid]) & set(reason_dat.columns))
                sspd = sspd.merge(reason_dat,on=join_key,how='left',suffixes=sfx)
        else:
            print('==> skipped because no file: %s is found or not cached'%reason_file_name)

    sspd = sspd.fillna('')#importance for .apply
    sspd['reason'] = sspd.apply(
        lambda x: merge_col_into_json(x,reason_exist,jsKey='reasons'),
        axis=1
    )


"""
reason function
"""
def reason_salesforce_hot_location(sspd: pd.DataFrame, jsKey='Additional Reason',**kwargs):
    reason_col_name = sys._getframe().f_code.co_name
    print('==>%s' % reason_col_name)
    datapath = kwargs['datapath']
    bid = kwargs['bid']
    fid = kwargs['fid']
    table_name = kwargs['reason_support_hot_loctation']
    reason_file = pj(datapath, table_name)
    rsdb = pd.read_csv(reason_file, index_col=0)[[bid]]
    print('%d hot locations loaded from %s' % (len(rsdb),reason_file))
    clpair = sspd.merge(rsdb, on=bid)
    print('==> Coverage:%1.3f' % (len(clpair) / len(sspd)))
    if clpair.empty:
        clpair = pd.DataFrame(columns=[fid, bid,reason_col_name])
    else:
        reason_desc = 'This location is very popular among all the buildings.'
        clpair[reason_col_name] = json.dumps({jsKey: [reason_desc]})
    return clpair[[fid,bid,reason_col_name]]


def reason_salesforce_similar_location(sspd: pd.DataFrame, jsKey='Addition Reason',**kwargs):
    reason_col_name = sys._getframe().f_code.co_name
    print('==>%s' % reason_col_name)
    datapath = kwargs['datapath']
    bid = kwargs['bid']
    fid = kwargs['fid']
    gid = 'cluster_id'
    table_name = kwargs['reason_support_item2item']
    i2i_file = pj(datapath, table_name)
    i2idb = pd.read_csv(i2i_file, index_col=0)[[bid, gid]]
    print('%d item2item loaded' % len(i2idb))

    table_name = kwargs['salesforce_pos_file']
    his_file = pj(datapath, table_name)
    hisdb = pd.read_csv(his_file, index_col=0)[[fid, bid]]
    print('%d history visit loaded' % len(hisdb))

    hisdb = hisdb.merge(i2idb, on=bid, suffixes=sfx)[[fid, gid]]  # bid is dropped
    # de-duplication, can be replaced by something like sortbyvalues
    hisdb = hisdb.drop_duplicates([fid, gid], keep='first')

    sspd = sspd.merge(i2idb, on=bid, suffixes=sfx)[[fid, bid, gid]]

    clpair = sspd.merge(hisdb, on=[fid, gid], suffixes=sfx)
    print('==> Coverage:%1.3f' % (len(clpair) / len(sspd)))

    if clpair.empty:
        clpair = pd.DataFrame(columns=[fid, bid, reason_col_name])
    else:
        reason_desc = 'This location is similar to the location your visited before.'
        clpair[reason_col_name] = json.dumps({jsKey: [reason_desc]})

    return clpair[[fid, bid, reason_col_name]]


def reason_salesforce_demand_x_inventory(sspd: pd.DataFrame, jsKey='Demand Signals',**kwargs):
    reason_col_name = sys._getframe().f_code.co_name
    print('==>%s' % reason_col_name)
    bid = kwargs['bid']
    fid = kwargs['fid']

    inv_file = kwargs['inventory_file']
    dtloader = data_process(datapath)
    inv_dat = dtloader.load_inventory(db='', dbname=inv_file)
    inv_col = dtloader.inv_col
    assert (bid == inv_col['bid'])
    inv_dat = inv_dat[[bid, inv_col['cap']]].rename(columns={inv_col['cap']: 'cap'})

    print('%d inventory loaded' % len(inv_dat))

    table_name = kwargs['demand_file']
    dtloader.load_demand(db='', dbname=table_name)
    demand_dat = dtloader.deduplicate_demand_tb(db='', save_dbname='')
    demand_col = dtloader.demand_col
    demand_dat = demand_dat[[demand_col['acc_col'], demand_col['req_desk']]].rename(
        columns={demand_col['req_desk']: 'req_desk',
                 demand_col['acc_col']: fid})

    print('%d demand loaded' % len(demand_dat))

    clpair = sspd.merge(inv_dat, on=bid, suffixes=sfx)[[fid, bid, 'cap']]
    clpair = clpair.merge(demand_dat, on=fid, suffixes=sfx)[[fid, bid, 'cap', 'req_desk']]
    clpair = clpair.fillna(0)

    clpair = clpair.loc[clpair['req_desk'] > 0]
    clpair = clpair.loc[clpair['cap'] > 0]

    if clpair.empty:
        clpair = pd.DataFrame(columns=[fid, bid, reason_col_name])
    else:
        clpair = clpair.loc[clpair['req_desk'].astype(int) <= clpair['cap'].astype(int)]
        print('==> Coverage:%1.3f' % (len(clpair) / len(sspd)))
        reason_desc = '[Size] The location available space(%d) can meet your requirement(%d).'
        clpair[reason_col_name] = clpair.apply(
            lambda x: reason_desc % (int(x['req_desk']), int(x['cap'])), axis=1)
        clpair[reason_col_name] = clpair[reason_col_name].apply(
            lambda x: json.dumps({jsKey: [x]})
        )

    return clpair[[fid, bid, reason_col_name]]
