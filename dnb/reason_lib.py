from dnb.header import *
from dnb.utils import *

sfx = ['','_right']

def set_xcom_var(ti, key, value):
    ti.xcom_push(key=key, value=value)


def get_xcom_var(ti, task_id, key):
    return ti.xcom_pull(task_ids=task_id, key=key)

def data_merge_for_city(city_reason_file_name,sub_reason_file_names,reason_names, var_task_space,**context):

    ti = context.get("ti")

    print('Merging reasons')
    sample_sspd = get_xcom_var(ti,var_task_space,'sspd')

    for reason_name,value in reason_names.items():
        # priority,useFLG = value["p"],value["useFLG"]
        reason_db = pd.read_csv(pjoin(datapath_mid,sub_reason_file_names[reason_name]),index_col=0)
        match_key = list(set([bid, cid]) & set(reason_db.columns))  # sometimes only location uuid is given
        sample_sspd = sample_sspd.merge(reason_db, on=match_key, how='left', suffixes=sfx)

    sample_sspd = sample_sspd.fillna('')
    print('Json format transforming...')
    sorted_reason_col_name = sorted(reason_names.items(), key=lambda x: x[1]['p'])
    sorted_reason_col_name = [c[0] for c in sorted_reason_col_name]
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
    print('##city: %s processing##' % citylongname[ind_city])
    comp_feat = pd.read_csv(pjoin(datapath, cfile[ind_city]))
    comp_loc = pd.read_csv(pjoin(datapath, clfile[ind_city]))
    loc_feat = pd.read_csv(pjoin(datapath, lfile))
    loc_feat = loc_feat.merge(comp_loc[[bid]].groupby(bid).first().reset_index(),
                              on=bid, suffixes=sfx)
    print('Global filtering')
    global_ft = global_filter(loc_feat=loc_feat)
    sub_loc_feat = global_ft.city_filter(city_name=cityname[ind_city]).end()

    sub_loc_feat_ww = sub_loc_feat.loc[sub_loc_feat['is_wework'] == True, :]
    sub_comp_loc = pd.merge(comp_loc, sub_loc_feat_ww[[bid]], on=bid,
                            suffixes=sfx)  # multi comp loc
    print('==> %d locations inside the city' % len(sub_loc_feat_ww))

    sspd = pd.read_csv(pjoin(datapath, ssfile[ind_city]), index_col=0)

    ti = context.get("ti")
    set_xcom_var(ti, key='comp_feat', value=comp_feat)
    set_xcom_var(ti, key='comp_loc', value=comp_loc)
    set_xcom_var(ti, key='loc_feat', value=loc_feat)
    set_xcom_var(ti, key='sub_loc_feat', value=sub_loc_feat)
    set_xcom_var(ti, key='sub_comp_loc', value=sub_comp_loc)
    set_xcom_var(ti, key='sspd', value=sspd)


def reason_similar_biz( sub_reason_col_name, sub_reason_file_name ,var_task_space, **context):
    print('Is there a company with similar biz inside the location?')
    ti = context.get("ti")
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
                                                                    reason='This location has a tenant company(%s) which is in the same industry as your company.')
    # explanar
    print('==> Coverage: %1.2f' % (len(sub_pairs) / total_pairs_num))
    sub_pairs.to_csv(sub_reason_file)

def reason_close_2_current_location(sub_reason_col_name, sub_reason_file_name ,var_task_space, **context):
    print('Close to current location')
    ti = context.get("ti")
    sspd = get_xcom_var(ti,var_task_space,'sspd')
    comp_feat = get_xcom_var(ti,var_task_space,'comp_feat')
    loc_feat = get_xcom_var(ti,var_task_space,'loc_feat')

    total_pairs_num = len(sspd)
    sub_reason_file = pjoin(datapath_mid, sub_reason_file_name)

    recall_com6 = sub_rec_location_distance(reason_col_name=sub_reason_col_name)
    sub_close_loc = recall_com6.get_reason(sspd=sspd, loc_feat=loc_feat, comp_feat=comp_feat, dist_thresh=3.2e3)

    print('==> Coverage: %1.2f' % (len(sub_close_loc) / total_pairs_num))
    sub_close_loc.to_csv(sub_reason_file)