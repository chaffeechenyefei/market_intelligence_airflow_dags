from dnb.header import *
from dnb.utils import *
import dnb.data_loader as data_loader

sfx = ['','_right']

def set_xcom_var(ti, key, value):
    ti.xcom_push(key=key, value=value)


def get_xcom_var(ti, task_id, key):
    return ti.xcom_pull(task_ids=task_id, key=key)


def prod_all_reason_in_one_func(ind_city,var_task_space, **context):
    sspd_file = pjoin(datapath_mid, ssfile[ind_city])
    if not os.path.isfile(sspd_file):
        skpFLG = True
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

    compstak_db = pd.read_csv(pjoin(datapath, compstak_file))[['tenant_id', 'expiration_date', 'city']]
    compstak_dnb = pd.read_csv(pjoin(datapath, compstak_dnb_match_file))[['tenant_id', cid, 'city']]
    compstak_db_city = compstak_db.loc[compstak_db['city'] == cityname[ind_city], :]
    compstak_dnb_city = compstak_dnb.loc[compstak_dnb['city'] == cityname[ind_city], :]
    print('==> %d compstak_db loaded' % len(compstak_db_city))

    comp_feat_normed = pd.read_csv(pjoin(datapath_mid, comp_feat_file), index_col=0)
    if 'city' in comp_feat_normed.columns:
        comp_feat_normed = comp_feat_normed.loc[comp_feat_normed['city'] == cityname[ind_city]]
    loc_feat_normed = pd.read_csv(pjoin(datapath_mid, loc_feat_file), index_col=0)
    if 'city' in loc_feat_normed.columns:
        loc_feat_normed = loc_feat_normed.loc[loc_feat_normed['city'] == cityname[ind_city]]
    print('==> normalized feature loaded')

    dlsub_ssfile_db = dlsub_ssfile[ind_city]


    reason_names = hdargs["reason_col_name"]
    sub_reason_file_names = {}
    for reason_name in reason_names.keys():
        if reason_names[reason_name]["useFLG"]:
            sub_reason_file_names[reason_name] = cityabbr[ind_city] + '_' + reason_name + hdargs['otversion']
            sub_reason_file_name = sub_reason_file_names[reason_name]
            exe_func = locals()[reason_name]
            exe_func( sub_reason_col_name=reason_name, sub_reason_file_name=sub_reason_file_name,var_task_space=var_task_space, **context )

    print('==> Merging reasons')
    city_reason_file_name = rsfile[ind_city]
    sample_sspd = sspd
    exist_reason = []
    for reason_name, value in reason_names.items():
        # priority,useFLG = value["p"],value["useFLG"]
        db_path = pjoin(datapath_mid, sub_reason_file_names[reason_name])
        if os.path.isfile(db_path):
            exist_reason.append(reason_name)
            reason_db = pd.read_csv(db_path, index_col=0)
            match_key = list(set([bid, cid]) & set(reason_db.columns))  # sometimes only location uuid is given
            sample_sspd = sample_sspd.merge(reason_db, on=match_key, how='left', suffixes=sfx)
        else:
            print('%s skipped because no file is found in %s' % (reason_name, str(db_path)))

    sample_sspd = sample_sspd.fillna('')
    print('Json format transforming...')
    sorted_reason_col_name = sorted(reason_names.items(), key=lambda x: x[1]['p'])
    sorted_reason_col_name = [c[0] for c in sorted_reason_col_name if c[0] in exist_reason]
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

    print('==> final %d data to be saved'%len(sample_sspd))
    sample_sspd.to_csv(pjoin(datapath_mid, city_reason_file_name))
    print('==> Done')

def data_merge_for_all_cities():
    print('merging results')
    dfs = []
    for filename in rsfile:
        db_path = pjoin(datapath_mid, filename)
        if os.path.isfile(db_path):
            dfs.append(pd.read_csv(db_path, index_col=0))
        else:
            print('Missing: %s'%str(db_path))

    dfs = pd.concat(dfs, axis=0).reset_index(drop=True)

    loc_df = dfs.groupby(bid, sort=True)[[bid]].first().reset_index(drop=True)

    k = list(range(len(loc_df)))
    pd_id = pd.DataFrame(np.array(k), columns=['building_id'])
    loc_df = pd.concat([loc_df, pd_id], axis=1)

    dfs = dfs[['company_id', 'similarity', 'note', 'algorithm', 'atlas_location_uuid']].merge(loc_df,
                                                                                              on='atlas_location_uuid',
                                                                                              how='left',
                                                                                              suffixes=['', '_right'])

    col_list = ['company_id', 'building_id', 'similarity', 'note', 'algorithm', 'atlas_location_uuid']
    dfs = dfs[col_list]

    if TEST_FLG:
        dfs.to_csv('sub_all_similarity_multi_test'+ hdargs["otversion"], index=False)
    else:
        dfs.to_csv('sub_all_similarity_multi'+ hdargs["otversion"], index=False)

    print('Done!')


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



def reason_similar_biz( sub_reason_col_name, sub_reason_file_name ,var_task_space, **context):
    print('Is there a company with similar biz inside the location?')
    ti = context.get("ti")
    skpFLG = get_xcom_var(ti,var_task_space,'skp_FLG')
    if skpFLG:
        print('skipped!')
        return

    sspd = get_xcom_var(ti,var_task_space,'sspd')
    comp_feat = get_xcom_var(ti,var_task_space,'comp_feat')
    sub_comp_loc = get_xcom_var(ti, var_task_space, 'sub_comp_loc')

    total_pairs_num = len(sspd)
    sub_reason_file = pjoin(datapath_mid, sub_reason_file_name)

    matching_col = 'primary_sic_4_digit'  # 'primary_sic_2_digit_v2','major_industry_category'
    query_comp_loc = sspd[[bid, cid]]
    query_comp_loc = query_comp_loc.merge(comp_feat[[cid, matching_col]], on=cid, suffixes=sfx)

    recall_com = sub_rec_similar_company(comp_feat=comp_feat, comp_loc=sub_comp_loc,
                                          matching_col=matching_col, reason_col_name=sub_reason_col_name,
                                          bid=bid, cid=cid, cname='business_name')

    sub_pairs = recall_com.get_candidate_location_for_company_fast(query_comp_loc=query_comp_loc,
                                                                    reason='This location has a tenant company(%s) which is in the same industry(%s) as your company.')
    # explanar
    print('==> Coverage: %1.2f' % (len(sub_pairs) / total_pairs_num))
    sub_pairs.to_csv(sub_reason_file)


def reason_close_2_current_location(sub_reason_col_name, sub_reason_file_name ,var_task_space, **context):
    print('Close to current location')
    ti = context.get("ti")
    skpFLG = get_xcom_var(ti,var_task_space,'skp_FLG')
    if skpFLG:
        print('skipped!')
        return
    sspd = get_xcom_var(ti,var_task_space,'sspd')
    comp_feat = get_xcom_var(ti,var_task_space,'comp_feat')
    loc_feat = get_xcom_var(ti,var_task_space,'loc_feat')

    total_pairs_num = len(sspd)
    sub_reason_file = pjoin(datapath_mid, sub_reason_file_name)

    recall_com = sub_rec_location_distance(reason_col_name=sub_reason_col_name)
    sub_close_loc = recall_com.get_reason(sspd=sspd, loc_feat=loc_feat, comp_feat=comp_feat, dist_thresh=3.2e3)

    print('==> Coverage: %1.2f' % (len(sub_close_loc) / total_pairs_num))
    sub_close_loc.to_csv(sub_reason_file)


def reason_inventory_bom(sub_reason_col_name, sub_reason_file_name ,var_task_space, **context):
    print('Inventory bom')
    ti = context.get("ti")
    skpFLG = get_xcom_var(ti,var_task_space,'skp_FLG')
    if skpFLG:
        print('skipped!')
        return
    sspd = get_xcom_var(ti,var_task_space,'sspd')
    comp_feat = get_xcom_var(ti,var_task_space,'comp_feat')

    total_pairs_num = len(sspd)
    sub_reason_file = pjoin(datapath_mid, sub_reason_file_name)

    invdb = pd.read_csv(pjoin(datapath, inventory_file))

    recall_com = sub_rec_inventory_bom(invdb=invdb,
                                        reason='Inventory reason: The max reservable desks( %d ) of this location can hold your company.',
                                        bid=bid, cid=cid)
    sub_inventory_db = recall_com.get_reason(sspd=sspd, comp_feat=comp_feat, comp_col='emp_here',
                                              inv_col='max_reservable_capacity', reason_col=sub_reason_col_name)
    print('==> Coverage: %1.2f' % (len(sub_inventory_db) / total_pairs_num))
    sub_inventory_db.to_csv(sub_reason_file)

def reason_talent_score(sub_reason_col_name, sub_reason_file_name ,var_task_space, **context):
    print('Talent score')
    ti = context.get("ti")
    skpFLG = get_xcom_var(ti,var_task_space,'skp_FLG')
    if skpFLG:
        print('skipped!')
        return
    sspd = get_xcom_var(ti, var_task_space, 'sspd')

    total_pairs_num = len(sspd)
    sub_reason_file = pjoin(datapath_mid, sub_reason_file_name)

    dataloadCls = data_loader.data_process(root_path=hdargs["run_root"])
    talentdb = dataloadCls.load_talent()
    lscard = dataloadCls.load_location_scorecard_msa()

    recall_com = sub_rec_talent(talentdb=talentdb,lsdb=lscard,reason='Talent score here is %s.')
    sub_talent_db = recall_com.get_reason(sspd=sspd,reason_col=sub_reason_col_name)

    print('==> Coverage: %1.2f' % (len(sub_talent_db) / total_pairs_num))
    sub_talent_db.to_csv(sub_reason_file)


def reason_compstak(sub_reason_col_name, sub_reason_file_name ,var_task_space, **context):
    print('Compstak')
    ti = context.get("ti")
    skpFLG = get_xcom_var(ti,var_task_space,'skp_FLG')
    if skpFLG:
        print('skipped!')
        return
    sspd = get_xcom_var(ti, var_task_space, 'sspd')
    compstak_db_city = get_xcom_var(ti, var_task_space, 'compstak_db_city')
    compstak_dnb_city = get_xcom_var(ti, var_task_space, 'compstak_dnb_city')

    total_pairs_num = len(sspd)
    sub_reason_file = pjoin(datapath_mid, sub_reason_file_name)

    recall_com = sub_rec_compstak(cpstkdb=compstak_db_city, cpstkdnb=compstak_dnb_city,
                                   reason='Compstak reason: The lease will expire in %d months.',
                                   cid=cid, bid=bid)
    sub_compstak_db = recall_com.get_reason(sspd=sspd, reason_col=sub_reason_col_name)

    print('==> Coverage: %1.2f' % (len(sub_compstak_db) / total_pairs_num))
    sub_compstak_db.to_csv(sub_reason_file)

def reason_similar_company(sub_reason_col_name, sub_reason_file_name ,var_task_space, **context):
    print('Is there a similar company inside the recommended location?')
    ti = context.get("ti")
    skpFLG = get_xcom_var(ti,var_task_space,'skp_FLG')
    if skpFLG:
        print('skipped!')
        return
    sspd = get_xcom_var(ti, var_task_space, 'sspd')
    comp_feat = get_xcom_var(ti, var_task_space, 'comp_feat')
    sub_comp_loc = get_xcom_var(ti, var_task_space,'sub_comp_loc')
    # comp_loc = get_xcom_var(ti, var_task_space, 'comp_loc')
    comp_feat_normed = get_xcom_var(ti, var_task_space, 'comp_feat_normed')

    comp_feat_col = [c for c in comp_feat_normed.columns if c not in [cid, bid]]
    total_pairs_num = len(sspd)
    sub_reason_file = pjoin(datapath_mid, sub_reason_file_name)

    matching_col = 'primary_sic_6_digit'
    query_comp_loc = sspd[[bid, cid]]
    query_comp_loc = query_comp_loc.merge(comp_feat[[cid, matching_col]], on=cid, suffixes=sfx)

    recall_com5_ext = sub_rec_similar_company(comp_feat=comp_feat, comp_loc=sub_comp_loc,
                                              matching_col=matching_col, reason_col_name=sub_reason_col_name,
                                              bid=bid, cid=cid, cname='business_name')
    sub_sspd = recall_com5_ext.get_candidate_location_for_company_fast(query_comp_loc=query_comp_loc,
                                                                       reason='This location has a tenant company(%s) which is in the same industry as your company.')
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
                                                 reason_col_name=sub_reason_col_name, batch_size=5000)

    print('==> Coverage: %1.2f' % (len(sim_comp_name) / total_pairs_num))
    sim_comp_name.to_csv(sub_reason_file)

def reason_similar_location(sub_reason_col_name, sub_reason_file_name ,var_task_space, **context):
    print('Is the recommended location similar with its current one?')
    ti = context.get("ti")
    skpFLG = get_xcom_var(ti,var_task_space,'skp_FLG')
    if skpFLG:
        print('skipped!')
        return
    sspd = get_xcom_var(ti, var_task_space, 'sspd')
    comp_loc = get_xcom_var(ti, var_task_space, 'comp_loc')
    loc_feat = get_xcom_var(ti, var_task_space, 'loc_feat')

    total_pairs_num = len(sspd)
    sub_reason_file = pjoin(datapath_mid, sub_reason_file_name)

    cont_col_nameL = feature_column['cont_col_nameL']
    dummy_col_nameL = feature_column['dummy_col_nameL']
    recall_com = sub_rec_similar_location(cont_col_name=cont_col_nameL, dummy_col_name=dummy_col_nameL,
                                           reason_col_name=sub_reason_col_name, cid=cid, bid=bid)
    loc_comp_loc = recall_com.get_reason(sspd=sspd, comp_loc=comp_loc, loc_feat=loc_feat,
                                          reason='Location similar in: ', multi_flag=True)

    print('==> Coverage: %1.2f' % (len(loc_comp_loc) / total_pairs_num))
    loc_comp_loc.to_csv(sub_reason_file)


def reason_location_based(sub_reason_col_name, sub_reason_file_name ,var_task_space, **context):
    print('How is region?(Location based reason)')
    ti = context.get("ti")
    skpFLG = get_xcom_var(ti,var_task_space,'skp_FLG')
    if skpFLG:
        print('skipped!')
        return
    sub_loc_feat = get_xcom_var(ti, var_task_space, 'sub_loc_feat')
    sub_loc_feat_ww = get_xcom_var(ti, var_task_space, 'sub_loc_feat_ww')

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
    sub_loc_recall[sub_reason_col_name] = 'This building is at a location with great amenities: ' + sub_loc_recall[
        sub_reason_col_name] + '. '

    print('==> Coverage: %1.2f' % (len(sub_loc_recall) / len(sub_loc_feat_ww)))
    sub_loc_recall.to_csv(sub_reason_file)


def reason_model_based(sub_reason_col_name, sub_reason_file_name ,var_task_space, **context):
    print('Model based Reason(Implicit reason)')
    ti = context.get("ti")
    skpFLG = get_xcom_var(ti,var_task_space,'skp_FLG')
    if skpFLG:
        print('skipped!')
        return
    dlsub_ssfile_db_name = get_xcom_var(ti,var_task_space, 'dlsub_ssfile_db')
    total_pairs_num = get_xcom_var(ti, var_task_space, 'total_pairs_num')

    sub_reason_file = pjoin(datapath_mid, sub_reason_file_name)

    featTranslator = feature_translate()
    dlsubdat = pd.read_csv(pjoin(datapath, dlsub_ssfile_db_name), index_col=0)
    dlsubdat[sub_reason_col_name] = dlsubdat.apply(lambda row: featTranslator.make_sense(row['merged_feat']),
                                                   axis=1)
    dlsubdat = dlsubdat[[bid, cid, sub_reason_col_name]]

    print('==> Coverage: %1.2f' % (len(dlsubdat) / total_pairs_num))
    dlsubdat.to_csv(sub_reason_file)