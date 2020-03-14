from dnb.header import *
from dnb.utils import *
import dnb.data_loader as data_loader
import sys

sfx = ['','_right']

def set_xcom_var(ti, key, value):
    ti.xcom_push(key=key, value=value)


def get_xcom_var(ti, task_id, key):
    return ti.xcom_pull(task_ids=task_id, key=key)


def prod_prepare_data():
    ## average price data
    dtld = data_loader.data_process(root_path=datapath)
    dbname = pjoin(datapath,compstak_file)
    compstak_submarket_avg_price = dtld.load_submarket_avg_price(db='',dbname=dbname)
    filename = pjoin(datapath,compstak_submarket_file)
    compstak_submarket_avg_price.to_csv(filename)
    print('average price file: %s Done'%filename )

    ## top one valid lease
    compstak_TIM = dtld.load_compstak_aligned(db='',dbname=dbname)
    filename = pjoin(datapath,compstak_TIM_file)
    compstak_TIM.to_csv(filename)
    print('valid lease file: %s Done'%filename )

    ## capacity cdm
    dbname = pjoin(datapath,cdm_reservable_file)
    cdm_capacity = dtld.load_cdm_capacity(db='',dbname = dbname)
    filename = pjoin(datapath,cdm_capacity_file)
    cdm_capacity.to_csv(filename)
    print('cdm capacity file: %s Done'%filename)



def prod_all_reason_in_one_func(ind_city, **context):
    """
    Doing all the reasonings in one function sequentially.
    :param ind_city: 
    :param context: 
    :return: 
    """
    sspd_file = pjoin(datapath_mid, ssfile[ind_city])
    if not os.path.isfile(sspd_file):
        print('skipped')
        return

    """
    load data here
    """
    print('##city: %s processing##' % citylongname[ind_city])
    comp_feat = pd.read_csv(pjoin(datapath, cfile[ind_city]))
    comp_loc = pd.read_csv(pjoin(datapath_mid, clfile[ind_city]))
    loc_feat = pd.read_csv(pjoin(datapath, lfile))
    loc_feat = loc_feat.merge(comp_loc[[bid]].groupby(bid).first().reset_index(),
                              on=bid, suffixes=sfx)
    print('Global filtering')
    global_ft = global_filter(loc_feat=loc_feat)
    sub_loc_feat = global_ft.city_filter(city_name=cityname[ind_city]).end()

    sub_loc_feat_ww = sub_loc_feat.loc[sub_loc_feat['is_wework'] == True, :]
    # sub_comp_loc: company-location pair where location should belongs to ww
    sub_comp_loc = pd.merge(comp_loc, sub_loc_feat_ww[[bid]], on=bid,
                            suffixes=sfx)  # multi comp loc
    print('==> %d locations inside the city' % len(sub_loc_feat_ww))

    sspd = pd.read_csv(pjoin(datapath_mid, ssfile[ind_city]), index_col=0)
    total_pairs_num = len(sspd)
    print('==> %d pairs of recommendation score' % total_pairs_num)

    # compstak_db = pd.read_csv(pjoin(datapath, compstak_file))[
    #     ['tenant_id', 'expiration_date', 'effective_rent', 'city','latitude','longitude']]
    compstak_db = pd.read_csv(pjoin(datapath,compstak_TIM_file))[['tenant_id','transaction_size' ,'expiration_date', 'effective_rent', 'city','submarket','latitude','longitude']]
    compstak_submarket_price = pd.read_csv(pjoin(datapath,compstak_submarket_file))[['city','submarket','low_effective_rent']]

    # compstak_dnb = pd.read_csv(pjoin(datapath, compstak_dnb_match_file))[['tenant_id', cid, 'physical_city']].rename(
    #     columns={'physical_city': 'city', })
    compstak_dnb = pd.read_csv(pjoin(datapath, compstak_dnb_match_file))[['tenant_id', cid]]

    # compstak_db_city = compstak_db.loc[compstak_db['city'] == cityname[ind_city], :]
    compstak_db_city = compstak_db

    # compstak_dnb_city = compstak_dnb.loc[compstak_dnb['city'] == cityname[ind_city], :]
    compstak_dnb_city = compstak_dnb
    print('==> %d compstak_db loaded' % len(compstak_db_city))

    cdm_capacity = pd.read_csv(pjoin(datapath,cdm_capacity_file),index_col=0)
    print('==> %d cdm capacity loaded'%len(cdm_capacity))


    comp_feat_normed = pd.read_csv(pjoin(datapath_mid, comp_feat_file), index_col=0)
    if 'city' in comp_feat_normed.columns:
        comp_feat_normed = comp_feat_normed.loc[comp_feat_normed['city'] == cityname[ind_city]]
    loc_feat_normed = pd.read_csv(pjoin(datapath_mid, loc_feat_file), index_col=0)
    if 'city' in loc_feat_normed.columns:
        loc_feat_normed = loc_feat_normed.loc[loc_feat_normed['city'] == cityname[ind_city]]
    print('==> normalized feature loaded')

    dlsub_ssfile_db = dlsub_ssfile[ind_city]

    kwargs = dict(
        comp_feat = comp_feat,
        comp_loc = comp_loc,
        loc_feat = loc_feat,
        sub_loc_feat = sub_loc_feat,
        sub_comp_loc = sub_comp_loc,
        sspd = sspd,
        compstak_db_city = compstak_db_city,
        compstak_dnb_city = compstak_dnb_city,
        compstak_submarket_price = compstak_submarket_price,
        cdm_capacity = cdm_capacity,
        comp_feat_normed = comp_feat_normed,
        loc_feat_normed = loc_feat_normed,
        sub_loc_feat_ww = sub_loc_feat_ww,
        dlsub_ssfile_db = dlsub_ssfile_db,
        total_pairs_num = total_pairs_num,
    )

    reason_names = hdargs["reason_col_name"]
    sub_reason_file_names = {}
    for reason_name in reason_names.keys():
        sub_reason_file_names[reason_name] = cityabbr[ind_city] + '_' + reason_name + hdargs['otversion']
        if reason_names[reason_name]["useFLG"]:
            sub_reason_file_name = sub_reason_file_names[reason_name]
            #if produce, it will be removed.
            sub_reason_full_file_name = pj(datapath_mid,sub_reason_file_name)
            if os.path.exists(sub_reason_full_file_name):
                os.remove(sub_reason_full_file_name)
            jsKey = reason_names[reason_name]["rsKey"]
            exe_func = globals()[reason_name]
            exe_func( sub_reason_col_name=reason_name, sub_reason_file_name=sub_reason_file_name, jsKey = jsKey, **kwargs )

    print('==> Merging reasons')
    city_reason_file_name = rsfile[ind_city]
    sample_sspd = sspd
    exist_reason = []
    for reason_name, value in reason_names.items():
        db_path = pjoin(datapath_mid, sub_reason_file_names[reason_name])
        if os.path.isfile(db_path) and value["cache"]:#if cache exist then use cache reason
            reason_db = pd.read_csv(db_path, index_col=0)
            if len(reason_db) > 0:
                exist_reason.append(reason_name)
                match_key = list(set([bid, cid]) & set(reason_db.columns))  # sometimes only location uuid is given
                sample_sspd = sample_sspd.merge(reason_db, on=match_key, how='left', suffixes=sfx)
        else:
            print('%s skipped because no file is found in %s or not cached enable.' % (reason_name, str(db_path)))

    sample_sspd = sample_sspd.fillna('')
    print('Json format transforming...')
    sorted_reason_col_name = sorted(reason_names.items(), key=lambda x: x[1]['p'])
    sorted_reason_col_name = [c[0] for c in sorted_reason_col_name if c[0] in exist_reason]
    if len(sample_sspd) > 0:
        sample_sspd['reason'] = sample_sspd.apply(
            lambda x: merge_str_2_json_rowise_reformat_v3(row=x, src_cols=sorted_reason_col_name, jsKey='reasons'),
            axis=1)

        sample_sspd['filter'] = ''
        # filter_cols = [c for c in hdargs["filter_col_name"].keys() if c in sample_sspd.columns ]
        # if len(filter_cols) > 0:
        #     sample_sspd['filter'] = sample_sspd.apply(
        #         lambda x: merge_str_2_json_for_filter( row=x, src_cols= filter_cols, jsKey='filters',default=True),
        #         axis =1
        #     )
        # else:
        #     sample_sspd['filter'] = ''
    else:
        sample_sspd['reason'] = ''
        sample_sspd['filter'] = ''

    sample_sspd[cid] = sample_sspd[cid].astype(int)
    sample_sspd = sample_sspd.rename(columns={
        "reason": "note"
    })
    sample_sspd['building_id'] = sample_sspd['atlas_location_uuid'].apply(lambda x: hash(x))
    sample_sspd['algorithm'] = 'model_wide_and_deep'

    col_list = [cid, 'building_id', 'similarity', 'note', 'atlas_location_uuid', 'algorithm','filter']#+filter
    sample_sspd = sample_sspd[col_list]
    sample_sspd['similarity'] = sample_sspd['similarity'].round(4)

    print('==> final %d data to be saved'%len(sample_sspd))
    sample_sspd.to_csv(pjoin(datapath_mid, city_reason_file_name))
    print('==> Done')

def data_merge_for_all_cities():
    """
    Merge all the files of cities into one and format it for output.
    :return: 
    """
    print('merging results')
    dfs = []
    for filename in rsfile:
        db_path = pjoin(datapath_mid, filename)
        if os.path.isfile(db_path):
            dfs.append(pd.read_csv(db_path, index_col=0))
        else:
            print('Missing: %s'%str(db_path))

    dfs = pd.concat(dfs, axis=0).reset_index(drop=True)

    loc_df = dfs.drop_duplicates(bid)[[bid]].reset_index(drop=True)
    # loc_df = dfs.groupby(bid, sort=True)[[bid]].first().reset_index(drop=True)

    k = list(range(len(loc_df)))
    pd_id = pd.DataFrame(np.array(k), columns=['building_id'])
    loc_df = pd.concat([loc_df, pd_id], axis=1)

    dfs_list = [cid, 'similarity', 'note', 'algorithm', 'atlas_location_uuid','filter']
    dfs = dfs[dfs_list].merge(loc_df, on=bid, how='left', suffixes=['', '_right'])

    col_list = [cid, 'building_id', 'similarity', 'note', 'algorithm', bid, 'filter']
    dfs = dfs[col_list]

    if TEST_FLG:#result/sub_all_similarity_multi[_test][_200106.csv]
        dfs.to_csv( pj( datapath,hdargs["final_file_name"] + '_test' + hdargs["otversion"]), index=False)
    else:
        dfs.to_csv(pj(datapath, hdargs["final_file_name"]+ hdargs["otversion"]), index=False)

    print('dnb_atlas score saved...')

    add_info_dat = pd.read_csv(pj(datapath_mid, salesforce_dnb_info_file), index_col=0)[
        ['sfdc_account_id', cid, 'account_name', 'company_name', 'city', 'zip_code', 'state', 'longitude', 'latitude']]

    dfs = dfs.merge(add_info_dat, on=cid, suffixes=sfx)

    dfs['selected'] = dfs['duns_number'].apply(lambda x: True if int(x) in [8900629, 965324655, 63407694, 97916953,
                                                                            41311187, 80282091, 968544028, 81116354,
                                                                            92952713,
                                                                            80582771, 75824558, 42472380, 42019874,
                                                                            80762681, 965522118,
                                                                            65786502, 72148831, 80660916, 81209201,
                                                                            196532191, 47132640,
                                                                            43271786, 796371933, 81009915,
                                                                            872951269, 80614333, 80180758, 51631832,
                                                                            74157331, 80775063,
                                                                            13895459, 80388989, 80482577,
                                                                            21588858, 78521935, 80385047,
                                                                            78777722, 78777721, 835524919, 96733699,
                                                                            39590514,
                                                                            177265824, 80414232, 78777764,
                                                                            ] else False)

    dfs = dfs[
        ['sfdc_account_id', 'account_name', 'building_id', bid, 'similarity', 'note', 'algorithm', cid, 'company_name',
         'city', 'zip_code', 'state', 'longitude', 'latitude','selected','filter']]
    len_dup = len(dfs)
    print('Dedupliation')
    dfs = dfs.sort_values(['sfdc_account_id',bid,'similarity']).drop_duplicates(['sfdc_account_id',bid],keep='last')
    print('#%d --> #%d after deduplication'%(len_dup,len(dfs)))

    today = datetime.date.today()
    dfs.to_csv(pj(datapath,'result/recommendation_reason_%s.csv'%str(today)),index=False)
    print('Done! %d saved'%len(dfs))


def data_merge_for_city(city_reason_file_name,sub_reason_file_names,reason_names, var_task_space,**context):
    ti = context.get("ti")
    skpFLG = get_xcom_var(ti,var_task_space,'skp_FLG')
    if skpFLG:
        print('skipped!')
        return

    print('Merging reasons')
    sample_sspd = get_xcom_var(ti,var_task_space,'sspd')

    exist_reason = []
    for reason_name,value in reason_names.items():
        # priority,useFLG = value["p"],value["useFLG"]
        db_path = pjoin(datapath_mid, sub_reason_file_names[reason_name])
        if os.path.isfile(db_path):
            exist_reason.append(reason_name)
            reason_db = pd.read_csv( db_path,index_col=0)
            match_key = list(set([bid, cid]) & set(reason_db.columns))  # sometimes only location uuid is given
            sample_sspd = sample_sspd.merge(reason_db, on=match_key, how='left', suffixes=sfx)
        else:
            print('%s skipped because no file is found in %s'%(reason_name,str(db_path)))


    sample_sspd = sample_sspd.fillna('')
    print('Json format transforming...')
    sorted_reason_col_name = sorted(reason_names.items(), key=lambda x: x[1]['p'])
    sorted_reason_col_name = [c[0] for c in sorted_reason_col_name if c[0] in exist_reason ]
    sample_sspd['reason'] = sample_sspd.apply(
        lambda x: merge_str_2_json_rowise_reformat(row=x, src_cols=sorted_reason_col_name, jsKey='reasons',
                                                   target_phss=['Location similar in: ', 'Implicit reason: ']), axis=1)

    sample_sspd[cid] = sample_sspd[cid].astype(int)
    sample_sspd = sample_sspd.rename(columns={
        "reason": "note", "duns_number": "company_id"
    })
    sample_sspd['building_id'] = sample_sspd['atlas_location_uuid'].apply(lambda x: hash(x))
    sample_sspd['algorithm'] = 'model_wide_and_deep'

    col_list = ['company_id', 'building_id', 'similarity', 'note', 'atlas_location_uuid', 'algorithm']
    sample_sspd = sample_sspd[col_list]
    sample_sspd['similarity'] = sample_sspd['similarity'].round(4)

    print(len(sample_sspd))

    sample_sspd.to_csv(pjoin(datapath_mid, city_reason_file_name))


def data_prepare(ind_city,**context):
    ti = context.get("ti")
    """
    Assert whether should prod the city first
    Use prediction score file as criterion
    """
    sspd_file = pjoin(datapath_mid, ssfile[ind_city])
    if not os.path.isfile(sspd_file):
        set_xcom_var(ti, key='skp_FLG', value=True)
        return
    else:
        set_xcom_var(ti, key='skp_FLG', value=False)

    print('##city: %s processing##' % citylongname[ind_city])
    comp_feat = pd.read_csv(pjoin(datapath, cfile[ind_city]))
    comp_loc = pd.read_csv(pjoin(datapath_mid, clfile[ind_city]))
    loc_feat = pd.read_csv(pjoin(datapath, lfile))
    loc_feat = loc_feat.merge(comp_loc[[bid]].groupby(bid).first().reset_index(),
                              on=bid, suffixes=sfx)
    print('Global filtering')
    global_ft = global_filter(loc_feat=loc_feat)
    sub_loc_feat = global_ft.city_filter(city_name=cityname[ind_city]).end()

    sub_loc_feat_ww = sub_loc_feat.loc[sub_loc_feat['is_wework'] == True, :]
    # sub_comp_loc: company-location pair where location should belongs to ww
    sub_comp_loc = pd.merge(comp_loc, sub_loc_feat_ww[[bid]], on=bid,
                            suffixes=sfx)  # multi comp loc
    print('==> %d locations inside the city' % len(sub_loc_feat_ww))

    sspd = pd.read_csv(pjoin(datapath_mid, ssfile[ind_city]), index_col=0)
    total_pairs_num = len(sspd)
    print('==> %d pairs of recommendation score'%total_pairs_num)

    compstak_db = pd.read_csv(pjoin(datapath, compstak_file))[['tenant_id', 'expiration_date', 'city']]
    compstak_dnb = pd.read_csv(pjoin(datapath, compstak_dnb_match_file))[['tenant_id', cid, 'city']]
    compstak_db_city = compstak_db.loc[compstak_db['city'] == cityname[ind_city], :]
    compstak_dnb_city = compstak_dnb.loc[compstak_dnb['city'] == cityname[ind_city], :]
    print('==> %d compstak_db loaded'%len(compstak_db_city))

    comp_feat_normed = pd.read_csv(pjoin(datapath_mid, comp_feat_file), index_col=0)
    if 'city' in comp_feat_normed.columns:
        comp_feat_normed = comp_feat_normed.loc[ comp_feat_normed['city']==cityname[ind_city] ]
    loc_feat_normed = pd.read_csv(pjoin(datapath_mid, loc_feat_file), index_col=0)
    if 'city' in loc_feat_normed.columns:
        loc_feat_normed = loc_feat_normed.loc[ loc_feat_normed['city']==cityname[ind_city] ]
    print('==> normalized feature loaded')

    dlsub_ssfile_db = dlsub_ssfile[ind_city]


    set_xcom_var(ti, key='comp_feat', value=comp_feat)
    set_xcom_var(ti, key='comp_loc', value=comp_loc)
    set_xcom_var(ti, key='loc_feat', value=loc_feat)
    set_xcom_var(ti, key='sub_loc_feat', value=sub_loc_feat)
    set_xcom_var(ti, key='sub_comp_loc', value=sub_comp_loc)
    set_xcom_var(ti, key='sspd', value=sspd)
    set_xcom_var(ti, key='compstak_db_city', value=compstak_db_city)
    set_xcom_var(ti, key='compstak_dnb_city', value=compstak_dnb_city)
    set_xcom_var(ti, key='comp_feat_normed', value=comp_feat_normed)
    set_xcom_var(ti, key='loc_feat_normed', value=loc_feat_normed)
    set_xcom_var(ti, key='sub_loc_feat_ww', value=sub_loc_feat_ww)
    set_xcom_var(ti, key='dlsub_ssfile_db', value=dlsub_ssfile_db)
    set_xcom_var(ti, key='total_pairs_num', value=total_pairs_num)

def reason_similar_biz( sub_reason_col_name, sub_reason_file_name, jsKey ,**kwargs):
    """
    json++
    + jsKey, function name
    """
    reason_col_name = sys._getframe().f_code.co_name
    print('%s: Is there a company with similar biz inside the location?'%reason_col_name)

    sspd = kwargs['sspd']
    comp_feat = kwargs['comp_feat']
    sub_comp_loc = kwargs['sub_comp_loc']

    total_pairs_num = len(sspd)
    sub_reason_file = pjoin(datapath_mid, sub_reason_file_name)

    matching_col = 'primary_sic_4_digit_v2'  # 'primary_sic_2_digit_v2','major_industry_category'
    query_comp_loc = sspd[[bid, cid]]
    query_comp_loc = query_comp_loc.merge(comp_feat[[cid, matching_col]], on=cid, suffixes=sfx)

    recall_com = sub_rec_similar_company(comp_feat=comp_feat, comp_loc=sub_comp_loc,
                                          matching_col=matching_col, reason_col_name=sub_reason_col_name,
                                          bid=bid, cid=cid, cname='business_name')

    # reason_desc = '[Industry Friendly Location] The area of this location is great for the account\'s industry(%s). ' \
    #               'Companies such as %s in the same industry are already in this location.'
    reason_desc = 'Companies in the same industry (%s) are present in this location, including %s.'
    sub_pairs = recall_com.get_candidate_location_for_company_fast(query_comp_loc=query_comp_loc,
                                                                    reason=reason_desc,
                                                                   jsFLG=True,jsKey=jsKey)
    # explanar
    print('==> Coverage: %1.2f' % (len(sub_pairs) / total_pairs_num))
    sub_pairs.to_csv(sub_reason_file)


def reason_close_2_current_location(sub_reason_col_name, sub_reason_file_name, jsKey,**kwargs):
    """
    json++
    :param sub_reason_col_name: 
    :param sub_reason_file_name: 
    :param kwargs: 
    :return: 
    """
    reason_col_name = sys._getframe().f_code.co_name
    print('%s: Close to current location'%reason_col_name)
    sspd = kwargs['sspd']
    comp_feat = kwargs['comp_feat']
    loc_feat = kwargs['loc_feat']

    total_pairs_num = len(sspd)
    sub_reason_file = pjoin(datapath_mid, sub_reason_file_name)

    recall_com = sub_rec_location_distance(reason_col_name=sub_reason_col_name)
    sub_close_loc = recall_com.get_reason(sspd=sspd, loc_feat=loc_feat, comp_feat=comp_feat, dist_thresh=3.2e3,jsFLG=False,jsKey=jsKey)

    print('==> Coverage: %1.2f' % (len(sub_close_loc) / total_pairs_num))
    sub_close_loc.to_csv(sub_reason_file)


def reason_close_2_current_location_compstak(sub_reason_col_name, sub_reason_file_name, jsKey,**kwargs):
    """
    json++
    :param sub_reason_col_name: 
    :param sub_reason_file_name: 
    :param kwargs: 
    :return: 
    """
    reason_col_name = sys._getframe().f_code.co_name
    print('%s: Close to current location'%reason_col_name)
    sspd = kwargs['sspd']
    loc_feat = kwargs['loc_feat']
    compstak_db_city = kwargs['compstak_db_city']
    compstak_dnb_city = kwargs['compstak_dnb_city']

    total_pairs_num = len(sspd)
    sspd = sspd.merge(compstak_dnb_city[['tenant_id', cid]], on=cid)[['tenant_id',cid,bid]]
    compstak_db_tmp = compstak_db_city[['tenant_id','longitude','latitude']].dropna()
    sspd = sspd.merge(compstak_db_tmp, on='tenant_id')#[bid,cid,lat,lng]

    sub_reason_file = pjoin(datapath_mid, sub_reason_file_name)

    recall_com = sub_rec_location_distance(reason_col_name=sub_reason_col_name)
    sub_close_loc = recall_com.get_reason_with_sspd_geo(sspd=sspd, loc_feat=loc_feat, dist_thresh=3.2e3,jsFLG=True,jsKey=jsKey,isMile=True)

    sub_close_loc = sub_close_loc.drop_duplicates([cid,bid])

    print('==> Coverage: %1.2f' % (len(sub_close_loc) / total_pairs_num))
    sub_close_loc.to_csv(sub_reason_file)


def reason_compstak_x_cdm_inventory(sub_reason_col_name, sub_reason_file_name, jsKey, **kwargs):
    reason_col_name = sys._getframe().f_code.co_name
    print('%s: Inventory bom' % reason_col_name)
    sspd = kwargs['sspd']
    cdm_capacity = kwargs['cdm_capacity']
    compstak_db_city = kwargs['compstak_db_city']
    compstak_dnb_city = kwargs['compstak_dnb_city']


    total_pairs_num = len(sspd)
    sub_reason_file = pjoin(datapath_mid, sub_reason_file_name)

    cpstkdb = compstak_db_city[['tenant_id', 'city', 'expiration_date', 'transaction_size']]
    cpstkdnb = compstak_dnb_city[[cid, 'tenant_id']]
    dnb_demand = cpstkdnb.merge(cpstkdb, on='tenant_id', suffixes=sfx)
    dnb_demand['demand_desk'] = sspd['transaction_size'].astype(int)/100
    dnb_demand['demand_desk'] = dnb_demand['demand_desk'].astype(int)
    dnb_demand = dnb_demand.loc[lambda df: df['demand_desk'] > 0 ]

    sspd = sspd.merge(dnb_demand,on=cid,suffixes=sfx)

    sspd = sspd.merge(cdm_capacity,on=bid,suffixes=sfx).dropna(subset=['capacity_desk','demand_desk'])

    sspd = sspd.loc[lambda df: (df['available_at']<=df['expiration_date']) and (df['capacity_desk'] >= df['demand_size']/100 ) ]

    reason_desc = 'The capacity (%d) of this location can hold the client\'s company (%d).'

    sspd[sub_reason_col_name] = sspd.apply(lambda df:
                reason_desc%(int(df['capacity_desk']),int(df['demand_size'])),axis=1 )

    sspd = sspd.drop_duplicates([cid,bid])

    scKey = secondKey.Size
    dfKey = '%s,%s'%(jsKey,scKey)

    sspd[sub_reason_col_name] = sspd[sub_reason_col_name].apply(
        lambda x: json.dumps( {dfKey:[str(x)]} )
    )

    print('==> Coverage: %1.2f' % (len(sspd) / total_pairs_num))
    sspd.to_csv(sub_reason_file)

def reason_inventory_bom(sub_reason_col_name, sub_reason_file_name, jsKey, **kwargs):
    """
    json++
    :param sub_reason_col_name: 
    :param sub_reason_file_name: 
    :param kwargs: 
    :return: 
    """
    reason_col_name = sys._getframe().f_code.co_name
    print('%s: Inventory bom'%reason_col_name)
    sspd = kwargs['sspd']
    comp_feat =kwargs['comp_feat']
    # jsKey = 'Portfolio signal'
    # rsKey = hdargs["reason_col_name"][sys._getframe().f_code.co_name]["rsKey"]
    # assert(rsKey == jsKey )
    filter_col = 'filter_size_dnb'
    filter_col = filter_col if filter_col in hdargs["filter_col_name"].keys() else None

    total_pairs_num = len(sspd)
    sub_reason_file = pjoin(datapath_mid, sub_reason_file_name)

    invdb = pd.read_csv(pjoin(datapath, inventory_file))

    recall_com = sub_rec_inventory_bom(invdb=invdb,
                                        reason='[Size] The max reservable desks( %d ) of this location can hold your company according to DnB.',
                                        bid=bid, cid=cid)
    sub_inventory_db = recall_com.get_reason(sspd=sspd, comp_feat=comp_feat, comp_col='emp_here',
                                              inv_col='max_reservable_capacity', reason_col=sub_reason_col_name,
                                             filter_col=filter_col, jsFLG=False,jsKey=jsKey)
    print('==> Coverage: %1.2f' % (len(sub_inventory_db) / total_pairs_num))
    sub_inventory_db.to_csv(sub_reason_file)


def reason_talent_score(sub_reason_col_name, sub_reason_file_name, jsKey, **kwargs):
    """
    json++
    :param sub_reason_col_name: 
    :param sub_reason_file_name: 
    :param kwargs: 
    :return: 
    """
    reason_col_name = sys._getframe().f_code.co_name
    print('%s: Talent score'%reason_col_name)
    sspd = kwargs['sspd']
    # jsKey = 'Additional Reasons'

    total_pairs_num = len(sspd)
    sub_reason_file = pjoin(datapath_mid, sub_reason_file_name)

    dataloadCls = data_loader.data_process(root_path=hdargs["run_root"])
    talentdb = dataloadCls.load_talent()
    lscard = dataloadCls.load_location_scorecard_msa()

    recall_com = sub_rec_talent(talentdb=talentdb,lsdb=lscard,reason='[Talent] Talent score here is %s.')
    sub_talent_db = recall_com.get_reason(sspd=sspd,reason_col=sub_reason_col_name,jsFLG=True,jsKey=jsKey)

    print('==> Coverage: %1.2f' % (len(sub_talent_db) / total_pairs_num))
    sub_talent_db.to_csv(sub_reason_file)



def reason_compstak(sub_reason_col_name, sub_reason_file_name, jsKey, **kwargs):
    """
    json++
    :param sub_reason_col_name: 
    :param sub_reason_file_name: 
    :param kwargs: 
    :return: 
    """
    reason_col_name = sys._getframe().f_code.co_name
    print('%s: Compstak'%reason_col_name)
    sspd = kwargs['sspd']
    compstak_db_city = kwargs['compstak_db_city']
    compstak_dnb_city = kwargs['compstak_dnb_city']

    total_pairs_num = len(sspd)
    sub_reason_file = pjoin(datapath_mid, sub_reason_file_name)

    recall_com = sub_rec_compstak(cpstkdb=compstak_db_city, cpstkdnb=compstak_dnb_city,
                                   reason='The client\'s nearby lease will expire in %d months.',
                                   cid=cid, bid=bid)
    sub_compstak_db = recall_com.get_reason(sspd=sspd, reason_col=sub_reason_col_name,jsFLG=True,jsKey=jsKey)

    print('==> Coverage: %1.2f' % (len(sub_compstak_db) / total_pairs_num))
    sub_compstak_db.to_csv(sub_reason_file)


def reason_compstak_timing(sub_reason_col_name, sub_reason_file_name, jsKey, **kwargs):
    """
    json++
    :param sub_reason_col_name: 
    :param sub_reason_file_name: 
    :param kwargs: 
    :return: 
    """
    reason_col_name = sys._getframe().f_code.co_name
    print('%s: Compstak' % reason_col_name)
    sspd = kwargs['sspd']
    compstak_db_city = kwargs['compstak_db_city']
    compstak_dnb_city = kwargs['compstak_dnb_city']

    total_pairs_num = len(sspd)
    sub_reason_file = pjoin(datapath_mid, sub_reason_file_name)

    recall_com = sub_rec_compstak(cpstkdb=compstak_db_city, cpstkdnb=compstak_dnb_city,
                                  reason='The client\'s nearby lease will expire in %d months.',
                                  cid=cid, bid=bid)
    sub_compstak_db = recall_com.get_reason(sspd=sspd, reason_col=sub_reason_col_name, jsFLG=True, jsKey=jsKey)

    sub_compstak_db = sub_compstak_db.drop_duplicates([cid,bid])

    print('==> Coverage: %1.2f' % (len(sub_compstak_db) / total_pairs_num))
    sub_compstak_db.to_csv(sub_reason_file)

def reason_compstak_pricing(sub_reason_col_name, sub_reason_file_name, jsKey, **kwargs):
    """
    json++
    :param sub_reason_col_name: 
    :param sub_reason_file_name: 
    :param kwargs: 
    :return: 
    """
    reason_col_name = sys._getframe().f_code.co_name
    print('%s: Compstak' % reason_col_name)
    sspd = kwargs['sspd']
    compstak_db_city = kwargs['compstak_db_city']
    compstak_dnb_city = kwargs['compstak_dnb_city']
    compstak_submarket_price = kwargs['compstak_submarket_price']

    total_pairs_num = len(sspd)
    sub_reason_file = pjoin(datapath_mid, sub_reason_file_name)

    reason_desc = 'The company is currently paying $%1.1f per RSF which is higher than the submarket\'s average of $%1.1f. '

    recall_com = sub_rec_compstak_price(cpstkdb=compstak_db_city, cpstkdnb=compstak_dnb_city,submarketprice= compstak_submarket_price,
                                  reason=reason_desc,
                                  cid=cid, bid=bid)

    sub_compstak_db = recall_com.get_reason(sspd=sspd, reason_col=sub_reason_col_name, jsFLG=True, jsKey=jsKey)

    sub_compstak_db = sub_compstak_db.drop_duplicates([cid,bid])

    print('==> Coverage: %1.2f' % (len(sub_compstak_db) / total_pairs_num))
    sub_compstak_db.to_csv(sub_reason_file)


def reason_similar_company(sub_reason_col_name, sub_reason_file_name, jsKey, **kwargs):
    """
    json++
    :param sub_reason_col_name: 
    :param sub_reason_file_name: 
    :param kwargs: 
    :return: 
    """
    reason_col_name = sys._getframe().f_code.co_name
    print('%s: Is there a similar company inside the recommended location?'%reason_col_name)
    sspd = kwargs['sspd']
    comp_feat = kwargs[ 'comp_feat']
    sub_comp_loc = kwargs['sub_comp_loc']
    comp_feat_normed = kwargs['comp_feat_normed']

    comp_feat_col = [c for c in comp_feat_normed.columns if c not in [cid, bid,'city','label']]
    total_pairs_num = len(sspd)
    sub_reason_file = pjoin(datapath_mid, sub_reason_file_name)

    matching_col = 'primary_sic_6_digit'
    query_comp_loc = sspd[[bid, cid]]
    query_comp_loc = query_comp_loc.merge(comp_feat[[cid, matching_col]], on=cid, suffixes=sfx)

    recall_com5_ext = sub_rec_similar_company(comp_feat=comp_feat, comp_loc=sub_comp_loc,
                                              matching_col=matching_col, reason_col_name=sub_reason_col_name,
                                              bid=bid, cid=cid, cname='business_name')
    sub_sspd = recall_com5_ext.get_candidate_location_for_company_fast(query_comp_loc=query_comp_loc)
    # explanar
    sub_sspd = sspd.merge(sub_sspd[[cid, bid]], on=[cid, bid], suffixes=sfx)
    print('Shrinkage ratio: %1.2f' % (len(sub_sspd) / len(sspd)))
    """
    Note: comp_loc is used as grd truth for a location to find what kind of companies are inside
    In this case, only wework locations are considered. Thus, no need to use the huge comp_loc relationship.
    """
    recall_com = sub_rec_similar_company_v2(comp_loc=sub_comp_loc, sspd=sub_sspd, thresh=0.05)
    sim_comp_name = recall_com.get_reason_batch(comp_feat=comp_feat, comp_feat_col=comp_feat_col,
                                                 comp_feat_normed=comp_feat_normed,
                                                 reason_col_name=sub_reason_col_name, batch_size=5000,jsFLG=True,jsKey=jsKey)

    print('==> Coverage: %1.2f' % (len(sim_comp_name) / total_pairs_num))
    sim_comp_name.to_csv(sub_reason_file)


def reason_similar_location(sub_reason_col_name, sub_reason_file_name, jsKey ,**kwargs):
    """
    json++
    :param sub_reason_col_name: 
    :param sub_reason_file_name: 
    :param kwargs: 
    :return: 
    """
    reason_col_name = sys._getframe().f_code.co_name
    print('%s: Is the recommended location similar with its current one?'%reason_col_name)
    sspd =  kwargs['sspd']
    comp_loc = kwargs['comp_loc']
    loc_feat = kwargs['loc_feat']


    total_pairs_num = len(sspd)
    sub_reason_file = pjoin(datapath_mid, sub_reason_file_name)

    cont_col_nameL = feature_column['cont_col_nameL']
    dummy_col_nameL = feature_column['dummy_col_nameL']
    recall_com = sub_rec_similar_location(cont_col_name=cont_col_nameL, dummy_col_name=dummy_col_nameL,
                                           reason_col_name=sub_reason_col_name, cid=cid, bid=bid)
    loc_comp_loc = recall_com.get_reason(sspd=sspd, comp_loc=comp_loc, loc_feat=loc_feat,
                                          reason='Location similar in: ', multi_flag=True, jsFLG=False,jsKey=jsKey)

    print('==> Coverage: %1.2f' % (len(loc_comp_loc) / total_pairs_num))
    loc_comp_loc.to_csv(sub_reason_file)


def reason_location_based_v2(sub_reason_col_name, sub_reason_file_name,jsKey , **kwargs):
    """
    json++
    + jskey, function name
    """
    scKey = secondKey.GA.value
    reason_col_name = sys._getframe().f_code.co_name
    print('%s: How is region?(Location based reason)'%reason_col_name)
    sub_loc_feat = kwargs['sub_loc_feat']
    sub_loc_feat_ww = kwargs['sub_loc_feat_ww']
    jsFLG = True

    sub_reason_file = pjoin(datapath_mid, sub_reason_file_name)
    # 'num_retail_stores', 'num_doctor_offices',
    # 'num_eating_places', 'num_drinking_places', 'num_hotels', 'num_fitness_gyms',

    cond_cols = ['num_retail_stores', 'num_doctor_offices','num_eating_places',
                 'num_drinking_places', 'num_hotels', 'num_fitness_gyms']
    reasons = ['retail stores' , 'doctor\'s offices', 'restaurants', 'bars', 'hotels','gyms']
    recall_com = sub_rec_condition(sub_loc_feat, bid=bid)
    sub_loc_recall = recall_com.exfiltering_v2(cond_cols=cond_cols, ww_loc_pd=sub_loc_feat_ww ,reasons=reasons,percentile=0.5,
                                                    reason_col_name=sub_reason_col_name)
    dfKey = '%s,%s' % (jsKey, scKey)
    if jsFLG:
        sub_loc_recall[sub_reason_col_name] = sub_loc_recall[sub_reason_col_name].apply(
            lambda x: json.dumps( {dfKey:[x]} )
        )

    print('==> Coverage: %1.2f' % (len(sub_loc_recall) / len(sub_loc_feat_ww)))
    sub_loc_recall.to_csv(sub_reason_file)

def reason_location_based(sub_reason_col_name, sub_reason_file_name,jsKey , **kwargs):
    """
    json++
    + jskey, function name
    """
    reason_col_name = sys._getframe().f_code.co_name
    print('%s: How is region?(Location based reason)'%reason_col_name)
    sub_loc_feat = kwargs['sub_loc_feat']
    sub_loc_feat_ww = kwargs['sub_loc_feat_ww']
    jsFLG = True

    sub_reason_file = pjoin(datapath_mid, sub_reason_file_name)

    recall_com = sub_rec_condition(sub_loc_feat, bid=bid)
    sub_loc_recall_com2 = recall_com.exfiltering('num_fitness_gyms', percentile=0.5,
                                                 reason='There are enough gyms to work out',
                                                 reason_col_name=sub_reason_col_name)
    sub_loc_recall_com3 = recall_com.exfiltering('num_drinking_places', percentile=0.5,
                                                 reason='There are enough bars to have a drink',
                                                 reason_col_name=sub_reason_col_name)
    sub_loc_recall_com4 = recall_com.exfiltering('num_eating_places', percentile=0.5,
                                                 reason='There are enough restaurants to get food',
                                                 reason_col_name=sub_reason_col_name)
    print('==> %d, %d, %d will be merged' % (
        len(sub_loc_recall_com2), len(sub_loc_recall_com3), len(sub_loc_recall_com4)))

    sub_loc_recall = pd.concat([sub_loc_recall_com2, sub_loc_recall_com3, sub_loc_recall_com4], axis=0)
    sub_loc_recall = sub_loc_recall.merge(sub_loc_feat_ww[[bid]], on=bid,
                                          suffixes=sfx)
    # explanar:merge_rec_reason_rowise 需要在结尾加"."
    sub_loc_recall = merge_rec_reason_rowise(sub_loc_recall, group_cols=[bid],
                                             merge_col=sub_reason_col_name, sep='. ')
    sub_loc_recall[sub_reason_col_name] = '[Amenity] This building is at a location with great amenities: ' + sub_loc_recall[
        sub_reason_col_name] + '. '
    if jsFLG:
        sub_loc_recall[sub_reason_col_name] = sub_loc_recall[sub_reason_col_name].apply(
            lambda x: json.dumps( {jsKey: [x] } )
        )

    print('==> Coverage: %1.2f' % (len(sub_loc_recall) / len(sub_loc_feat_ww)))
    sub_loc_recall.to_csv(sub_reason_file)


def reason_model_based(sub_reason_col_name, sub_reason_file_name, jsKey,**kwargs):
    """
    json++
    """
    reason_col_name = sys._getframe().f_code.co_name
    print('%s: Model based Reason(Implicit reason)'%reason_col_name)
    dlsub_ssfile_db_name =  kwargs['dlsub_ssfile_db']
    total_pairs_num =  kwargs['total_pairs_num']
    jsFLG = False

    sub_reason_file = pjoin(datapath_mid, sub_reason_file_name)

    dlsub_file_path = pjoin(datapath_mid, dlsub_ssfile_db_name)
    if not os.path.isfile(dlsub_file_path):
        print('no files %s found' % dlsub_file_path)
    featTranslator = feature_translate()
    dlsubdat = pd.read_csv(dlsub_file_path, index_col=0)
    if jsFLG:
        dlsubdat[sub_reason_col_name] = dlsubdat.apply(lambda row: json.dumps(featTranslator.make_sense_json(row['merged_feat'],jsKey=jsKey)),
                                                       axis=1)
    else:
        dlsubdat[sub_reason_col_name] = dlsubdat.apply(lambda row: featTranslator.make_sense(row['merged_feat']),
                                                       axis=1)

    dlsubdat = dlsubdat[[bid, cid, sub_reason_col_name]]

    print('==> Coverage: %1.2f' % (len(dlsubdat) / total_pairs_num))
    dlsubdat.to_csv(sub_reason_file)

def reason_price_based(sub_reason_col_name, sub_reason_file_name, jsKey ,**kwargs):
    """
    json++
    :param sub_reason_col_name: 
    :param sub_reason_file_name: 
    :param kwargs: 
    :return: 
    """
    reason_col_name = sys._getframe().f_code.co_name
    print('%s: Price based'%reason_col_name)
    sspd = kwargs['sspd']
    compstak_db_city = kwargs['compstak_db_city']
    compstak_dnb_city = kwargs['compstak_dnb_city']
    jsFLG = False

    # filter_col = 'filter_price'
    # filter_col =  filter_col if filter_col in hdargs["filter_col_name"].keys() else None
    filter_col = None

    total_pairs_num = len(sspd)
    sub_reason_file = pjoin(datapath_mid, sub_reason_file_name)

    invdb = pd.read_csv(pjoin(datapath, inventory_file))

    recall_com = sub_rec_price(cpstkdb=compstak_db_city, cpstkdnb=compstak_dnb_city,invdb=invdb,
                                   reason='[Price] The price of wework location is cheaper than your current location.',
                                   cid=cid, bid=bid)
    sub_compstak_db = recall_com.get_reason(sspd=sspd, reason_col=sub_reason_col_name, filter_col=filter_col,jsFLG=jsFLG,jsKey=jsKey)

    print('==> Coverage: %1.2f' % (len(sub_compstak_db) / total_pairs_num))
    sub_compstak_db.to_csv(sub_reason_file)


def reason_demand_x_inventory(sub_reason_col_name, sub_reason_file_name, jsKey, **kwargs):
    """
    json++
    :param sub_reason_col_name: 
    :param sub_reason_file_name: 
    :param kwargs: 
    :return: 
    """
    reason_col_name = sys._getframe().f_code.co_name
    print('%s: Demand x inventory'%reason_col_name)
    sspd = kwargs['sspd']
    total_pairs_num = len(sspd)
    sub_reason_file = pjoin(datapath_mid, sub_reason_file_name)
    # jsKey = 'Demand Signals'
    jsFLG = False
    filter_col = 'filter_size_demand'
    filter_col = filter_col if filter_col in hdargs["filter_col_name"].keys() else None

    recall_com = sub_rec_demand_x_inventory(root_path=datapath,invdbname=inventory_file,
                                            sfxdnbname=salesforce_dnb_match_file,demdbname=demand_file,
                                            reason='[Size] The location available space(%d) can meet your requirement(%d).')
    sub_pair = recall_com.get_reason(sspd=sspd,reason_col=sub_reason_col_name,filter_col=filter_col,jsFLG=jsFLG,jsKey=jsKey)

    print('==> Coverage: %1.2f' % (len(sub_pair) / total_pairs_num))
    sub_pair.to_csv(sub_reason_file)
