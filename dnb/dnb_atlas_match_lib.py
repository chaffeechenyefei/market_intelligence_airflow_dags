from dnb.header import *
from dnb.utils import *
from dnb.data_loader import data_process
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
    pdl = pd.read_csv(pj(datapath, ls_card),index_col=0)
    set_xcom_var(ti, key='ls_card', value=pdl)
    print('Done')

def data_load_ww_geohash(ls_card, precision,**context):
    ti = context.get("ti")
    print('==> Loading location scorecard:%s'%ls_card)
    pdl = pd.read_csv(pj(datapath, ls_card),index_col=0)
    pdl = pdl.loc[pdl['is_wework']==True]
    geohash(pdl,precision=precision)
    set_xcom_var(ti, key='ls_card', value=pdl)
    print('Done')


def data_load_geohash(ls_card, precision, **context):
    ti = context.get("ti")
    print('==> Loading location scorecard:%s' % ls_card)
    pdl = pd.read_csv(pj(datapath, ls_card), index_col=0)
    geohash(pdl, precision=precision)
    set_xcom_var(ti, key='ls_card', value=pdl)
    print('Done')


def prod_dnb_city_name_list(**op_kwargs):
    data_path = op_kwargs['data_path']
    dbname = op_kwargs['dbname']
    apps = op_kwargs['apps']

    dnb_path = pj(data_path,dbname)

    filelist = os.listdir(dnb_path)
    filelist = [c for c in filelist if c.split('.')[-1] in ['csv']]

    dbs = []
    for file in filelist:
        city_name = file.replace('.csv', '').replace('dnb_', '')
        db = pd.read_csv(pj(dnb_path, file), index_col=0)
        db = db[['msa', 'physical_city']]
        print('==> %s size: %d' % (city_name, len(db)))
        if len(db) == 0:
            continue
        db = db.groupby(['msa', 'physical_city']).first().reset_index()
        db['filename'] = file
        db['same'] = db['physical_city'].apply(lambda x: True if x == city_name else False)
        db['short_name'] = db['physical_city'].apply(lambda x: x.replace(' ', '_'))
        dbs.append(db)

    dbs = pd.concat(dbs, axis=0)

    same_dbs = dbs.loc[dbs['same']==True]
    print('Err:%d' % len(dbs.loc[dbs['same'] == False]))
    dbs.to_csv(pj(datapath_mid, 'dnb_msa_city_list'+apps))

    same_dbs = same_dbs.groupby(['physical_city','filename','short_name']).first().reset_index()[['physical_city','filename','short_name']]
    same_dbs.to_csv(pj(datapath_mid,'dnb_city_list'+apps))
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


def dnb_atlas_match_ww(cfile,clfile,precision,dist_thresh,**context):
    """
    This function only get pairs of Dnb and ww location. Thus, it is safe for predition only.
     All the locations can be used to compare because DnB data is a bit dirty.
    """
    ti = context.get("ti")

    print('==> Reading location scorecard')
    pdl = get_xcom_var(ti,task_id=context['var_task_space'],key='ls_card')

    outfile = clfile
    print('==> DnB Atlas matching for%s'%cfile)
    pdc = pd.read_csv(pj(datapath, cfile))

    linkCL = fuzzy_geosearchv3(pdc, pdl, precision=precision, thresh=dist_thresh)
    print('==> Matched size %d'%len(linkCL))

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
    #datLoc_city = cityfilter(datComp, datLoc)
    datLoc_city = city_state_filter(datComp, datLoc)
    print(len(datComp), len(datLoc_city))
    datComp_city = datComp[['duns_number', 'longitude', 'latitude']]
    datLoc_city = datLoc_city[['atlas_location_uuid', 'longitude', 'latitude']]

    geohash(datComp_city, precision)
    geohash(datLoc_city, precision)
    linkCL = calcLinkTablev2(datComp_city, datLoc_city, dist_thresh=thresh)

    return linkCL

def fuzzy_geosearchv3(datComp, datLoc, precision=5, thresh=500):
    """
    Same as fuzzy_geosearchv2, except:
    city filtering still added.
    """
    print('Initial company num:', len(datComp))
    # datLoc_city = datLoc
    datLoc_city = cityfilter(datComp, datLoc)
    print(len(datComp), len(datLoc_city))
    datComp_city = datComp[['duns_number', 'longitude', 'latitude']]
    datLoc_city = datLoc_city[['atlas_location_uuid', 'longitude', 'latitude']]

    geohash(datComp_city, precision)
    geohash(datLoc_city, precision)
    linkCL = calcLinkTablev2(datComp_city, datLoc_city, dist_thresh=thresh)

    return linkCL

def cityfilter(datComp, datLoc):
    city = datComp.groupby(['physical_city']).first().reset_index()['physical_city']
    print(len(city))
    pdatLoc = pd.merge(datLoc, city, how='inner', left_on=['city'], right_on=['physical_city'],
                       suffixes=['_loc', '_comp']).reset_index(drop=True)
    return pdatLoc

def city_state_filter(datComp, datLoc):
    datComp['state'] = datComp['msa'].apply(lambda x: x.split(',')[-1].strip() )
    city = datComp.groupby(['physical_city','state']).first().reset_index()[['physical_city','state']]
    print(len(city))
    pdatLoc = pd.merge(datLoc, city, how='inner', left_on=['city','state'], right_on=['physical_city','state'],
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

"""
Codes for splitting
"""
def data_split():
    print('==> Data splitting')
    ratio = hdargs["ratio_data_split"]
    max_K = hdargs["maxK_region"]
    apps = hdargs["apps"]
    test_round = hdargs["test_round"]
    save_tr_name = hdargs["train_file"] + apps
    save_tt_name = hdargs["test_file"] + apps
    cid = hdargs["cid"]
    bid = hdargs["bid"]

    citynameabbr = cityabbr

    clfile = [c + apps for c in citynameabbr]

    trdats = []
    ttdats = []

    for ind_city,filename in enumerate(clfile):
        print('Processing city: %s'%filename)
        cldat = pd.read_csv(pjoin(datapath,filename))
        #unique location list
        locdat = cldat.groupby(bid).first().reset_index()
        trlocdat = locdat.sample(frac=ratio).reset_index(drop=True)
        trlocdat['fold'] = 0
        ttlocdat = locdat.merge(trlocdat, on=bid, how='left', suffixes=sfx)
        ttlocdat = ttlocdat[ttlocdat['fold'].isnull()].reset_index()
        trlocdat = trlocdat[[bid]]
        ttlocdat = ttlocdat[[bid]]
        print('location for train: %d, for test: %d '%(len(trlocdat),len(ttlocdat)))
        #attach label
        trlocdat['fold'] = 0
        ttlocdat['fold'] = 2

        #attach label into location-company pairs
        trttlocdat = pd.concat([trlocdat, ttlocdat], axis=0).reset_index(drop=True)
        cldat = cldat.merge(trttlocdat, on=bid, how='left', suffixes=sfx)[[bid,cid,'fold']]
        cldat['city'] = ind_city
        print('location-company pairs for train: %d, for test %d'%(len(cldat[cldat['fold']==0]),len(cldat[cldat['fold']==2])))

        #saveing trdats
        trdats.append(cldat)

        #operate on ttdats
        cldat = cldat[cldat['fold'] == 2]

        print('inner loop for test expanding...')
        for i in range(test_round):
            print('###Round %d ###'%i)
            fn = lambda obj: obj.loc[np.random.choice(obj.index, 1, True),:]
            tbA = cldat.groupby(bid).apply(fn).reset_index(drop=True)[[cid, bid]]
            print('1.len of tbA %d:'%len(tbA))
            fn = lambda obj: obj.loc[np.random.choice(obj.index, max_K, True),:]
            tbB = cldat.groupby(bid).apply(fn).reset_index(drop=True)[[cid, bid]]
            print('1.len of tbB %d'%len(tbB))


            ###======================Pos=============================###
            tbA['mk'] = 'A'
            tbB = tbB.merge(tbA,on=[cid,bid],how='left',suffixes=sfx)
            tbB = tbB[tbB['mk'].isnull()]
            print('2.len of tbB not included in tbA %d'%len(tbB))
            #we need to full fill the data
            tbB = tbB.groupby(bid).apply(fn).reset_index(drop=True)[[cid, bid]]
            tbB['mk'] = 'B'
            print('3.len of tbB full filled again %d'%len(tbB))
            #in case tbB cut some locations from tbA, lets shrink tbA
            tblocB = tbB.groupby(bid).first().reset_index()
            print('4.len of locations in tbB %d'%len(tblocB))
            tbA = tbA.merge(tblocB,on=bid,how='left',suffixes=sfx)
            tbA = tbA[tbA['mk_right'].notnull()][[cid, bid,'mk']].reset_index(drop=True)
            print('4.len of tbA with common locations of tbB %d'%len(tbA))

            ###======================Neg=============================###
            tbAA = pd.concat([tbA,tbA.sample(frac=1).reset_index()\
                       .rename(columns={cid:cid+'_n',bid: bid+'_n','mk':'mk_n'})]
                      ,axis=1)
            print('5.len of negpair %d'%len(tbAA))
            tbAA = tbAA.merge(cldat,\
                       left_on=[ cid+'_n',bid],right_on=[cid,bid],\
                       how='left', suffixes = sfx)

            tbC = tbAA[tbAA[cid+'_right'].isnull()][[cid+'_n',bid]]\
                    .rename(columns={cid+'_n':cid})
            print('6.len of neg data %d'%len(tbC))

            #in case tbC cut some locations from tbA and tbB
            tbC['mk'] = 'C'
            tblocC = tbC.groupby(bid).first().reset_index()
            print('6.locations in neg data %d'%len(tblocC))
            tbA = tbA.merge(tblocC,on=bid,how='left',suffixes=sfx)
            tbA = tbA[tbA['mk_right'].notnull()][[cid, bid,'mk']].reset_index(drop=True)
            print('final tbA len %d'%len(tbA))

            tbB = tbB.merge(tblocC,on=bid,how='left',suffixes=sfx)
            tbB = tbB[tbB['mk_right'].notnull()][[cid, bid,'mk']].reset_index(drop=True)
            print('final tbB len %d'%len(tbB))

            tbA = tbA.sort_values(by=bid)
            tbB = tbB.sort_values(by=bid)
            tbC = tbC.sort_values(by=bid)

            assert(len(tbA)==len(tbC) and len(tbB)==len(tbA)*max_K)

            result = pd.concat([tbA, tbB, tbC], axis=0).reset_index(drop=True)
            result['city'] = ind_city
            print(len(result))

            ttdats.append(result)

    trdats = pd.concat(trdats, axis=0).reset_index(drop=True)
    ttdats = pd.concat(ttdats, axis=0).reset_index(drop=True)

    trdats.to_csv(pjoin(datapath_mid, save_tr_name))
    ttdats.to_csv(pjoin(datapath_mid, save_tt_name))



"""
Produce prediction file
"""
def prod_prediction_pair(**op_kwargs):
    save_filename = op_kwargs['save_filename']
    print('Loading dnb account within salesforce')
    pdc_all = pd.read_csv(pjoin(datapath_mid, salesforce_dnb_file))[[cid, 'city']]
    print('Loading location which has been embedded')
    loc_emb_feat_name = 'location_feat_emb_' + hdargs["dnb_dnn_cmd"]["model"] + '.csv'
    loc_ww = pd.read_csv(pj(datapath_mid,loc_emb_feat_name),index_col=0)

    for ind_city, str_city in enumerate(cityname):
        # pdcl = pd.read_csv(pjoin(datapath_mid, clfile[ind_city]))[[bid, cid]]
        pdc = pdc_all.loc[pdc_all['city'] == str_city]
        tot_comp = len(pdc)

        print('Total %d company found in %s from salesforce' % (tot_comp, str_city))
        if tot_comp == 0:
            print('skipped')
            continue
            
        pdc[bid] = 'a'
        # in case of multi-mapping

        all_loc_name = loc_ww.loc[loc_ww['city']==str_city][[bid]]
        tot_loc = len(all_loc_name)
        print('Total %d locations belonged to ww in %s' % (tot_loc, str_city))

        if tot_loc == 0:
            print('skipped')
            continue

        all_loc_name['key'] = 0
        pdc['key'] = 0

        testing_pair = pd.merge(pdc, all_loc_name, on='key', how='left',
                                suffixes=['_left', '_right']).reset_index(drop=True)

        print('testing pairs: %d, location number:%d, company number:%d' % (
        len(testing_pair), len(all_loc_name), len(pdc)))

        testing_pair = testing_pair.rename(
            columns={bid+'_left': 'groundtruth', bid+'_right': bid})
        testing_pair = testing_pair[[cid, bid, 'groundtruth']]
        testing_pair['label'] = (testing_pair[bid] == testing_pair['groundtruth'])
        testing_pair = testing_pair[[cid, bid, 'label']]

        testing_pair.to_csv(pjoin(datapath_mid, (save_filename % cityabbr[ind_city])))


    print('Done')

