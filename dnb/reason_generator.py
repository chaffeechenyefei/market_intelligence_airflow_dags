from dnb.header import *
from dnb.utils import *


# def read_data(**kwargs):
#     """
#
#     :param kwargs:
#     :return:
#     """
#     kwargs[kwargs_key].xcom_push(key='',value=)
# cid = 'duns_number'
# bid = 'atlas_location_uuid'
sfx = ['', '_right']


cfile = origin_comp_file
lfile = hdargs['ls_card']
clfile = [c + hdargs['apps'] for c in cityabbr]
cityname = citylongname
comp_feat_file = 'company_feat' + hdargs['apps']
comp_feat_normed = pd.read_csv(pjoin(datapath, comp_feat_file), index_col=0)
loc_feat_file = 'location_feat' + hdargs['apps']
loc_feat_normed = pd.read_csv(pjoin(datapath, loc_feat_file), index_col=0)

compstak_db = pd.read_csv(pjoin(datapath, compstak_file))[['tenant_id', 'expiration_date', 'city']]
compstak_dnb = pd.read_csv(pjoin(datapath, compstak_dnb_match_file))[['tenant_id', cid, 'city']]

ssfile = ['all_ww_' + c.replace(hdargs['apps'], '') + '_similarity' + hdargs['apps'] for c in clfile]
rsfile = ['z_reason_' + c + '_similarity' + hdargs['otversion'] for c in cityabbr]
dlsub_ssfile = ['dlsub_' + c for c in ssfile]

reason_col_name = [
    ('reason_similar_biz', 1, True),  # sub_pairs
    ('reason_location_based', 7, True),  # sub_loc_recall
    ('reason_model_based', 8, True),  # dlsubdat
    ('reason_similar_location', 6, True),
    ('reason_similar_company', 5, True),
    ('reason_close_2_current_location', 2, True),
    ('reason_inventory_bom', 3, True),
    ('reason_compstak', 4, True),
]

def reason_similar_biz():
    print('1. Is there a company with similar biz inside the location?')
    for ind_city in range(len(cityname)):
        print('##city: %s processing##' % cityname[ind_city])
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
        total_pairs_num = len(sspd)
        print('Loading %d similarity score pairs' % total_pairs_num)

        sub_reason_col_name, _, usedFLG = reason_col_name[0]
        sub_reason_file_name = cityabbr[ind_city] + '_' + sub_reason_col_name + hdargs['otversion']
        sub_reason_file = pjoin(datapath_mid, sub_reason_file_name)

        matching_col = 'primary_sic_4_digit'  # 'primary_sic_2_digit_v2','major_industry_category'
        query_comp_loc = sspd[[bid, cid]]
        query_comp_loc = query_comp_loc.merge(comp_feat[[cid, matching_col]], on=cid, suffixes=sfx)

        recall_com1 = sub_rec_similar_company(comp_feat=comp_feat, comp_loc=sub_comp_loc,
                                              matching_col=matching_col, reason_col_name=sub_reason_col_name,
                                              bid=bid, cid=cid, cname='business_name')

        sub_pairs = recall_com1.get_candidate_location_for_company_fast(query_comp_loc=query_comp_loc,
                                                                        reason='This location has a tenant company(%s) which is in the same industry as your company.')
        # explanar
        print('==> Coverage: %1.2f' % (len(sub_pairs) / total_pairs_num))
        sub_pairs.to_csv(sub_reason_file)
