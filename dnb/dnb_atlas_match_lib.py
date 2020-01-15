from dnb.header import *
from dnb.utils import *
import pandas as pd
import pygeohash as pgh
from math import *

sfx = ['','_right']

def set_xcom_var(ti, key, value):
    ti.xcom_push(key=key, value=value)


def get_xcom_var(ti, task_id, key):
    return ti.xcom_pull(task_ids=task_id, key=key)


def data_load(ls_card, **context):
    ti = context.get("ti")
    print('==> Loading location scorecard:%s'%ls_card)
    pdl = pd.read_csv(pj(datapath, ls_card))
    set_xcom_var(ti, key='ls_card', value=pdl)
    print('Done')



def dnb_atlas_match(cfile,clfile,precision,dist_thresh,**context):
    ti = context.get("ti")

    print('==> Reading location scorecard')
    pdl = get_xcom_var(ti,task_id=context['var_task_space'],key='ls_card')

    outfile = clfile
    print('==> DnB Atlas matching for%s'%cfile)
    pdc = pd.read_csv(pj(datapath, cfile))

    linkCL = fuzzy_geosearchv2(pdc, pdl, precision=precision, thresh=dist_thresh)

    linkCL.to_csv(pj(datapath_mid, outfile), index=None, header=True)
    print('Done')


def fuzzy_geosearchv2(datComp, datLoc, precision=5, thresh=500):
    """
    Each company can have multiply location in range controlled by precision and threshold.
    :param datComp: 
    :param datLoc: 
    :param precision: 
    :param thresh: 
    :return: 
    """
    print('Initial company num:', len(datComp))
    datLoc_city = cityfilter(datComp, datLoc)
    print(len(datComp), len(datLoc_city))
    datComp_city = datComp[['duns_number', 'longitude', 'latitude']]
    datLoc_city = datLoc_city[['atlas_location_uuid', 'longitude', 'latitude']]

    geohash(datComp_city, precision)
    geohash(datLoc_city, precision)
    linkCL = calcLinkTablev2(datComp_city, datLoc_city, dist_thresh=thresh)

    return linkCL

def cityfilter(datComp, datLoc):
    city = datComp.groupby(['physical_city'], as_index=False)['physical_city'].agg({'cnt': 'count'})
    print(len(city))
    pdatLoc = pd.merge(datLoc, city, how='inner', left_on=['city'], right_on=['physical_city'],
                       suffixes=['_loc', '_comp']).reset_index(drop=True)
    return pdatLoc

def geohash(data, precision=6):
    data['geohash'] = data.apply(lambda row: pgh.encode(row['longitude'], row['latitude'], precision=precision), axis=1)

def geo_distance(lng1, lat1, lng2, lat2):
    lng1, lat1, lng2, lat2 = map(radians, [lng1, lat1, lng2, lat2])
    dlon = lng2 - lng1
    dlat = lat2 - lat1
    a = sin(dlat / 2) ** 2 + cos(lat1) * cos(lat2) * sin(dlon / 2) ** 2
    dis = 2 * asin(sqrt(a)) * 6371 * 1000
    return dis

def calcLinkTablev2(datComp, datLoc, dist_thresh=500,verbose=True):
    if not verbose:
        print('merging...')
    df_cartesian = pd.merge(datLoc, datComp, on='geohash', how='left', suffixes=['_loc', '_comp'])
    if not verbose:
        print(list(df_cartesian.columns))
        print(len(df_cartesian))
        print('calc geo dist...')

    if dist_thresh > 0:
        df_cartesian['geo_distance'] = df_cartesian.apply(
            lambda row: geo_distance(row['longitude_comp'], row['latitude_comp'], row['longitude_loc'],
                                     row['latitude_loc']), axis=1)
        df_loc_comp = df_cartesian[df_cartesian['geo_distance'] <= dist_thresh]
    else:
        df_loc_comp = df_cartesian

    result = df_loc_comp[['duns_number', 'atlas_location_uuid', 'longitude_loc', 'latitude_loc']]
    num_used_loc = len(result.groupby('atlas_location_uuid').first().reset_index())
    num_used_comp = len(result.groupby('duns_number').first().reset_index())
    num_loc = len(datLoc)
    num_comp = len(datComp)
    print('location used: %d, location total: %d, coverage:%0.3f' % (num_used_loc, num_loc, num_used_loc / num_loc))
    print('company used: %d, company total: %d, coverage:%0.3f' % (num_used_comp, num_comp, num_used_comp / num_comp))
    return result

"""
Codes for normalizaiton
"""
def data_normalizaiton():
    print('Data normalization')
    cfile = origin_comp_file
    apps = hdargs["apps"]
    app_date = apps.replace('.csv','')
    lfile = hdargs["ls_card"]  # It is fixed as input
    clfile = [c + apps for c in cityabbr]

    # print('Args:',datapath,apps,lfile,args.ratio)

    not_feat_col = feature_column['not_feat_col']
    cont_col_nameC = feature_column['cont_col_nameC']
    spec_col_nameC = feature_column['spec_col_nameC']
    cont_col_nameL = feature_column['cont_col_nameL']
    key_col_comp = feature_column['key_col_comp']
    key_col_loc = feature_column['key_col_loc']

    dummy_col_nameL = feature_column['dummy_col_nameL']
    dummy_col_nameC = feature_column['dummy_col_nameC']

    ##Multi training data generator(multi city)
    # 如果不合并所有数据在进行dummy 会出现一些category在某些城市不出现的情况，从而导致问题
    # 8-2分训练测试集

    dat_comp_pds = []
    dat_loc_pds = []

    pdl = pd.read_csv(pjoin(datapath, lfile))
    pdlls = []  # all location feat pd list
    pdccs = []
    for ind_city in range(5):
        print('processing city: %s' % citylongname[ind_city])
        pdc = pd.read_csv(pjoin(datapath, cfile[ind_city]))
        pdcl = pd.read_csv(pjoin(datapath_mid, clfile[ind_city]))

        # building features
        col_list = list(pdl.columns)
        pdll = pdl.merge(pdcl, how='inner', on=['atlas_location_uuid'], suffixes=['', '_right'])
        pdll = pdll.groupby(['atlas_location_uuid']).first().reset_index()
        pdll = pdll[col_list]
        pdlls.append(pdll)

        # company feature
        pdc['city'] = ind_city
        pdccs.append(pdc)

    # for loop end
    pdlls = pd.concat(pdlls, axis=0)
    pdccs = pd.concat(pdccs, axis=0)

    # building feature
    pdlls = pdlls.reset_index()
    proc_pdl = location_dat_process(pdlls, one_hot_col_name=dummy_col_nameL, cont_col_name=cont_col_nameL)

    # company feature
    pdccs = pdccs.reset_index()
    proc_pdc = comp_dat_process(pdccs, one_hot_col_name=dummy_col_nameC, cont_col_name=cont_col_nameC,
                                spec_col_name=spec_col_nameC)
    print(len(proc_pdc))

    print('start saving company and location feature...')

    XC_comp, XD_comp, Y_comp, c_comp_name, d_comp_name, y_comp_name = transpd2np_single(proc_pdc, cont_col_nameC,
                                                                                        not_feat_col,
                                                                                        id_col_name=key_col_comp)
    XC_loc, XD_loc, Y_loc, c_loc_name, d_loc_name, y_loc_name = transpd2np_single(proc_pdl['data'], cont_col_nameL,
                                                                                  not_feat_col, id_col_name=key_col_loc)

    # one hot explanation
    comp_one_hot_col_name = dummy_col_nameC  # ['major_industry_category', 'location_type', 'primary_sic_2_digit']
    loc_one_hot_col_name = dummy_col_nameL  # ['building_class']

    loc_coldict = {}
    for colname in loc_one_hot_col_name:
        loc_coldict[colname] = []
        for dummyname in d_loc_name:
            if dummyname.startswith(colname):
                catname = dummyname.replace(colname, '', 1)  # replace only once(for the sake of protection)
                loc_coldict[colname].append(catname[1:])

    comp_coldict = {}
    for colname in comp_one_hot_col_name:
        comp_coldict[colname] = []
        for dummyname in d_comp_name:
            if dummyname.startswith(colname):
                catname = dummyname.replace(colname, '', 1)  # replace only once(for the sake of protection)
                comp_coldict[colname].append(catname[1:])

    print(comp_coldict, loc_coldict)

    save_obj(comp_coldict, pjoin(datapath_mid, 'comp_feat_dummy_param' + app_date))
    save_obj(loc_coldict, pjoin(datapath_mid, 'loc_feat_dummy_param' + app_date))

    print('one hot description stored...')

    print('Start normalization')

    C_comp, S_comp = get_para_normalize_dat(XC_comp)
    C_loc, S_loc = get_para_normalize_dat(XC_loc)
    XC_comp = apply_para_normalize_dat(XC_comp, C_comp, S_comp)
    XC_loc = apply_para_normalize_dat(XC_loc, C_loc, S_loc)

    X_comp = np.concatenate([Y_comp, XC_comp, XD_comp], axis=1)
    X_loc = np.concatenate([Y_loc, XC_loc, XD_loc], axis=1)

    comp_norm_param = {
        'C_comp': C_comp,
        'S_comp': S_comp,
        'columns': c_comp_name
    }

    loc_norm_param = {
        'C_loc': C_loc,
        'S_loc': S_loc,
        'columns': c_loc_name
    }

    save_obj(comp_norm_param, pjoin(datapath_mid, 'comp_feat_norm_param' + app_date))
    save_obj(loc_norm_param, pjoin(datapath_mid, 'loc_feat_norm_param' + app_date))

    dat_comp_pd = pd.DataFrame(data=X_comp, columns=y_comp_name + c_comp_name + d_comp_name)
    dat_comp_pd = pd.concat([dat_comp_pd, proc_pdc[['city']]], axis=1)

    dat_loc_pd = pd.DataFrame(data=X_loc, columns=y_loc_name + c_loc_name + d_loc_name)

    print(dat_comp_pd.to_numpy().mean())
    print(dat_loc_pd.to_numpy()[:, 1:].mean())
    print(dat_comp_pd.shape)

    print('Done')

    dat_comp_pd.to_csv(pjoin(datapath_mid, 'company_feat' + apps))
    dat_loc_pd.to_csv(pjoin(datapath_mid, 'location_feat' + apps))
    print('All Done')

    print(dat_comp_pd.shape, dat_loc_pd.shape)
