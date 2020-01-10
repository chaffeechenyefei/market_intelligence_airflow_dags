import pandas as pd
import numpy as np
import pickle
import os
from sklearn.preprocessing import normalize
from sklearn.preprocessing import OneHotEncoder
from enum import Enum
from math import *
import tqdm
import json
import datetime

pjoin = os.path.join


# function_base
def getPosNegdat(dat):
    """
    dat: pos pair of data (location,company,geo,distance)
    return pos/neg pair of data, same structure of dat except one more column for label
    """
    shuffle_dat = dat.sample(frac=1).reset_index(drop=True)

    # shuffle_dat.head()

    twin_dat = dat.join(shuffle_dat, how='left', lsuffix='_left', rsuffix='_right')
    twin_dat = twin_dat[twin_dat['atlas_location_uuid_left'] != twin_dat['atlas_location_uuid_right']]
    print(len(twin_dat))
    twin_dat.head()

    neg_datA = twin_dat[['duns_number_left', 'atlas_location_uuid_right', 'longitude_loc_right', 'latitude_loc_right']]
    neg_datA = neg_datA.rename(
        columns={'duns_number_left': 'duns_number', 'atlas_location_uuid_right': 'atlas_location_uuid',
                 'longitude_loc_right': 'longitude_loc', 'latitude_loc_right': 'latitude_loc'})

    neg_datB = twin_dat[['duns_number_right', 'atlas_location_uuid_left', 'longitude_loc_left', 'latitude_loc_left']]
    neg_datB = neg_datB.rename(
        columns={'duns_number_right': 'duns_number', 'atlas_location_uuid_left': 'atlas_location_uuid',
                 'longitude_loc_left': 'longitude_loc', 'latitude_loc_left': 'latitude_loc'})

    neg_dat = pd.concat([neg_datA, neg_datB], axis=0)
    neg_dat['label'] = 0
    dat['label'] = 1
    res_dat = pd.concat(
        [dat[['duns_number', 'atlas_location_uuid', 'longitude_loc', 'latitude_loc', 'label']], neg_dat], axis=0)
    print('Neg dat num:', len(neg_dat), ';Pos dat num:', len(dat))
    return res_dat


def getPosNegdatv2_fast(dat):
    """
    dat: pos pair of data (location,company,geo,distance)
    return pos/neg pair of data, same structure of dat except one more column for label
    """
    shuffle_dat = dat.sample(frac=1).reset_index(drop=True)
    twin_dat = dat.join(shuffle_dat, how='left', lsuffix='_left', rsuffix='_right')

    pot_neg_datA = twin_dat[
        ['duns_number_left', 'atlas_location_uuid_right', 'longitude_loc_right', 'latitude_loc_right']] \
        .rename(columns={'duns_number_left': 'duns_number', 'atlas_location_uuid_right': 'atlas_location_uuid',
                         'longitude_loc_right': 'longitude_loc', 'latitude_loc_right': 'latitude_loc'})

    pot_neg_datB = twin_dat[
        ['duns_number_right', 'atlas_location_uuid_left', 'longitude_loc_left', 'latitude_loc_left']] \
        .rename(columns={'duns_number_right': 'duns_number', 'atlas_location_uuid_left': 'atlas_location_uuid',
                         'longitude_loc_left': 'longitude_loc', 'latitude_loc_left': 'latitude_loc'})

    pot_neg_dat = pd.concat([pot_neg_datA, pot_neg_datB], axis=0)
    pot_neg_dat['label'] = 0
    dat['label'] = 1

    # col alignment
    col_list = ['duns_number', 'atlas_location_uuid', 'label']
    dat = dat[col_list]
    pot_neg_dat = pot_neg_dat[col_list]

    # clean pos dat in neg dat
    neg_dat = pd.merge(pot_neg_dat, dat, on=['duns_number', 'atlas_location_uuid'], how='left',
                       suffixes=['', '_right']).reset_index(drop=True)
    neg_dat['label'] = neg_dat['label'].fillna(0)
    neg_dat = neg_dat[neg_dat['label_right'] != 1]

    print('Clean %d neg data into %d real neg data.' % (len(pot_neg_dat), len(neg_dat)))

    res_dat = pd.concat([dat, neg_dat], axis=0).reset_index(drop=True)
    # res_dat = res_dat.groupby(['duns_number', 'atlas_location_uuid'])['label'].max().reset_index()
    return res_dat

def getPosNegdatv2_fast_general(dat,colname:dict):
    """
    dat: pos pair of data (location,company,geo,distance)
    return pos/neg pair of data, same structure of dat except one more column for label
    general version of getPosNegdatv2_fast, need key column inputs
    colname = {
        'company':'duns_number',
        'location':'atlas_location_uuid'
    }
    """
    shuffle_dat = dat.sample(frac=1).reset_index(drop=True)
    twin_dat = dat.join(shuffle_dat, how='left', lsuffix='_left', rsuffix='_right')

    pot_neg_datA = twin_dat[
        [ colname['company']+'_left', colname['location'] + '_right']] \
        .rename(columns={colname['company']+'_left': 'duns_number', colname['location'] + '_right': 'atlas_location_uuid'})

    pot_neg_datB = twin_dat[
        [colname['company']+'_right', colname['location'] + '_left']] \
        .rename(columns={colname['company']+'_right': 'duns_number', colname['location'] + '_left': 'atlas_location_uuid'})

    pot_neg_dat = pd.concat([pot_neg_datA, pot_neg_datB], axis=0)
    pot_neg_dat['label'] = 0
    dat['label'] = 1

    # col alignment
    col_list = [colname['company'], colname['location'] , 'label']
    dat = dat[col_list]
    pot_neg_dat = pot_neg_dat[col_list]

    # clean pos dat in neg dat
    neg_dat = pd.merge(pot_neg_dat, dat, on=[colname['company'], colname['location']], how='left',
                       suffixes=['', '_right']).reset_index(drop=True)
    neg_dat['label'] = neg_dat['label'].fillna(0)
    neg_dat = neg_dat[neg_dat['label_right'] != 1]

    print('Clean %d neg data into %d real neg data.' % (len(pot_neg_dat), len(neg_dat)))

    res_dat = pd.concat([dat, neg_dat], axis=0).reset_index(drop=True)
    # res_dat = res_dat.groupby(['duns_number', 'atlas_location_uuid'])['label'].max().reset_index()
    return res_dat


def getPosNegdatv2(dat):
    """
    dat: pos pair of data (location,company,geo,distance)
    return pos/neg pair of data, same structure of dat except one more column for label
    """
    shuffle_dat = dat.sample(frac=1).reset_index(drop=True)

    # shuffle_dat.head()

    twin_dat = dat.join(shuffle_dat, how='left', lsuffix='_left', rsuffix='_right')

    pot_neg_datA = twin_dat[
        ['duns_number_left', 'atlas_location_uuid_right', 'longitude_loc_right', 'latitude_loc_right']] \
        .rename(columns={'duns_number_left': 'duns_number', 'atlas_location_uuid_right': 'atlas_location_uuid',
                         'longitude_loc_right': 'longitude_loc', 'latitude_loc_right': 'latitude_loc'})

    pot_neg_datB = twin_dat[
        ['duns_number_right', 'atlas_location_uuid_left', 'longitude_loc_left', 'latitude_loc_left']] \
        .rename(columns={'duns_number_right': 'duns_number', 'atlas_location_uuid_left': 'atlas_location_uuid',
                         'longitude_loc_left': 'longitude_loc', 'latitude_loc_left': 'latitude_loc'})

    pot_neg_dat = pd.concat([pot_neg_datA, pot_neg_datB], axis=0)
    pot_neg_dat['label'] = 0
    dat['label'] = 1
    # col alignment
    col_list = ['duns_number', 'atlas_location_uuid', 'label']
    dat = dat[col_list]
    pot_neg_dat = pot_neg_dat[col_list]
    res_dat = pd.concat([dat, pot_neg_dat], axis=0)
    res_dat = res_dat.groupby(['duns_number', 'atlas_location_uuid'])['label'].max().reset_index()
    return res_dat


def splitdat(dat, key_column=['duns_number'], right_colunm='atlas_location_uuid_tr', rate_tr=0.8):
    """
    split the <company,location> pair into training/testing dat
    """
    tr = dat.sample(frac=rate_tr)
    tt = pd.merge(dat, tr, on=key_column, how='left', suffixes=['', '_tr'])
    tt = tt[tt[right_colunm].isnull()]
    tt = tt[list(tr.columns)]
    print('Train dat:', len(tr), 'Test dat:', len(tt))
    return tr, tt


# data process
def onehotdat(dat, key_column: list, dummy_na=True):
    dat[key_column] = dat[key_column].astype(str)
    dum_dat = pd.get_dummies(dat[key_column], dummy_na=dummy_na)  # it has nan itself
    return dum_dat


def split2num(emp_range: str):
    max_emp_val = emp_range.replace(' ', '').split('-')
    if len(max_emp_val) < 2:
        return 10
    else:
        return float(max_emp_val[1])


def max_col(dat, col, minval=1):
    dat[col] = dat[col].apply(lambda r: max(r, minval))


def comp_dat_process(dat, one_hot_col_name, cont_col_name, spec_col_name, do_dummy=True):
    """
    pd -> company key,cont_feature,spec_feature,dum_feature
    """
    # one_hot_col_name = ['major_industry_category', 'location_type', 'primary_sic_2_digit']
    # spec_col_name = 'emp_here_range'
    # cont_col_name = ['emp_here', 'emp_total', 'sales_volume_us', 'square_footage']
    cont_col_name = [c for c in cont_col_name if c != spec_col_name]

    if do_dummy:
        print('doing one-hot...')
        dum_dat = onehotdat(dat, one_hot_col_name)

    print('extract continuous...')
    cont_dat = dat[cont_col_name].fillna(value=0).astype(float)

    print('specific feature')
    spec_dat = dat[spec_col_name].fillna(value='1-10').astype(str)
    spec_dat = spec_dat.apply(lambda row: split2num(row))

    max_col(cont_dat, 'emp_here', 1)

    if do_dummy:
        res_dat = dat[['duns_number', 'city']].join([cont_dat, spec_dat, dum_dat], how='left')
    else:
        res_dat = dat[['duns_number', 'city']].join([cont_dat, spec_dat], how='left')

    if do_dummy:
        assert (len(res_dat) == len(dum_dat))
    assert (len(res_dat) == len(cont_dat))
    assert (len(res_dat) == len(spec_dat))
    return res_dat


def location_dat_process(dat, one_hot_col_name, cont_col_name, do_dummy=True):
    """
    pd -> location key,cont_feature,dum_feature
    """
    # one_hot_col_name = ['building_class']
    # cont_col_name = ['score_predicted_eo', 'score_employer', 'num_emp_weworkcore', 'num_poi_weworkcore',
    #                  'pct_wwcore_employee', 'pct_wwcore_business', 'num_retail_stores', 'num_doctor_offices',
    #                  'num_eating_places', 'num_drinking_places', 'num_hotels', 'num_fitness_gyms',
    #                  'population_density', 'pct_female_population', 'median_age', 'income_per_capita',
    #                  'pct_masters_degree', 'walk_score', 'bike_score']
    if do_dummy:
        print('doing one-hot...')
        dum_dat = onehotdat(dat, one_hot_col_name, False)
        print(len(dum_dat))

    print('extract continuous...')
    cont_dat = dat[cont_col_name].fillna(value=0).astype(float)
    print(len(cont_dat))

    if do_dummy:
        res_dat = dat[['atlas_location_uuid']].join([cont_dat, dum_dat], how='left')
    else:
        res_dat = dat[['atlas_location_uuid']].join([cont_dat], how='left')
    print(len(res_dat))

    if do_dummy:
        assert (len(res_dat) == len(dum_dat))
    assert (len(res_dat) == len(cont_dat))

    if do_dummy:
        return {'data': res_dat,
                'cont_feat_num': len(list(cont_dat.columns)),
                'dum_feat_num': len(list(dum_dat.columns))}
    else:
        return {'data': res_dat,
                'cont_feat_num': len(list(cont_dat.columns))}


def normalize_dat_v2(trX, ttX, axis=0):
    center = trX.mean(axis=axis)
    center = np.expand_dims(center, axis)
    scale = trX.std(axis=axis)
    scale = np.expand_dims(scale, axis)

    trX = (trX - center) / scale
    ttX = (ttX - center) / scale
    return trX, ttX


def get_para_normalize_dat(trX, axis=0):
    center = trX.mean(axis=axis)
    scale = trX.std(axis=axis)
    scale += 1e-4
    return center, scale


def apply_para_normalize_dat(X, center, scale, axis=0):
    """
    X can be pd or numpy!
    """
    center = np.expand_dims(center, axis)
    scale = np.expand_dims(scale, axis)
    X = (X - center) / scale
    return X


def normalize_dat(trX, ttX, cols=5, axis=0):
    D = trX[:, :cols]
    center = D.mean(axis=axis)
    center = np.expand_dims(center, axis)
    scale = D.std(axis=axis)
    scale = np.expand_dims(scale, axis)

    trX[:, :cols] = (D - center) / scale
    ttX[:, :cols] = (ttX[:, :cols] - center) / scale


def save_obj(obj, name):
    with open(name + '.pkl', 'wb') as f:
        pickle.dump(obj, f, pickle.HIGHEST_PROTOCOL)


def load_obj(name):
    with open(name + '.pkl', 'rb') as f:
        return pickle.load(f)


def transpd2np_single(featdat, cont_col_name: list, not_feat_col: list, id_col_name: list):
    XC = featdat.loc[:, cont_col_name].to_numpy()
    out_col = not_feat_col + cont_col_name
    dum_col_name = [col for col in list(featdat.columns) if col not in out_col]
    XD = featdat.loc[:, dum_col_name].to_numpy()
    Y = featdat[id_col_name].to_numpy()
    return XC, XD, Y, cont_col_name, dum_col_name, id_col_name


def apply_dummy(coldict: dict, data):
    cat_list = []
    dummy_num = len(coldict)
    dummy_name = []
    for key in coldict:
        cat_list.append(np.array(coldict[key]))  # list of array for onehot engine
        dummy_name = dummy_name + [key + '_' + col for col in coldict[key]]  # full name of dummy col name

    enc = OneHotEncoder(handle_unknown='ignore', categories=cat_list)

    origin_dummy_col = [key for key in coldict]
    result = enc.fit_transform(data[origin_dummy_col]).toarray()
    # array to pd
    pd_new = pd.DataFrame(data=result, columns=dummy_name)
    return pd_new


# =======================================================================================================================
# =======================================================================================================================
# get_sub_recommend_reason_after_similarity
# =======================================================================================================================
# =======================================================================================================================
def generate_loc_type(comp_feat, comp_loc, matching_col,cid='duns_number',bid='atlas_location_uuid',cname='business_name'):
    # matching_col = 'major_industry_category'
    comp_type = comp_feat[[cid, matching_col,cname]].dropna()
    comp_type_location = pd.merge(comp_type, comp_loc[[cid, bid]], on=cid)

    loc_type = comp_type_location.groupby([bid, matching_col]).first().reset_index()[
        [bid, matching_col,cname]]
    return loc_type


class sub_rec_similar_company(object):
    def __init__(self, comp_feat, comp_loc, matching_col, reason_col_name='reason',bid='atlas_location_uuid',cid='duns_number',cname = 'business_name'):
        """
        comp_feat: original company information
        comp_loc: company-location affinities of a certain city
        matching_col = 'major_industry_category' big category
                    or 'primary_sic_2_digit' more detailed category
        """
        self.comp_feat = comp_feat
        self.comp_loc = comp_loc
        self.matching_col = matching_col
        self.reason_col_name = reason_col_name
        self.loc_type = generate_loc_type(comp_feat, comp_loc, matching_col,cid=cid,bid=bid,cname=cname)
        self.bid = bid
        self.cid = cid
        self.cname = cname

    def get_candidate_location_for_company(self, query_comp_feat, reason='similar company inside'):
        sub_pairs = pd.merge(query_comp_feat[[self.cid, self.matching_col]], self.loc_type, on=self.matching_col,
                             how='left', suffixes=['', '_right'])
        sub_pairs = sub_pairs[
            sub_pairs[self.bid].notnull()]  # sometimes a company may have no location to recommend
        sub_pairs[self.reason_col_name] = reason
        return sub_pairs

    def get_candidate_location_for_company_fast(self, query_comp_loc, reason='similar company(%s) inside', jsFLG=False , jsKey='A'):
        sub_pairs = pd.merge(query_comp_loc[[self.cid, self.bid, self.matching_col]], self.loc_type,
                             on=[self.bid, self.matching_col], suffixes=['', '_right'])
        sub_pairs = sub_pairs.dropna()
        if jsFLG:
            sub_pairs[self.reason_col_name] = sub_pairs[self.cname].apply(lambda x: json.dumps({jsKey:[(reason % str(x))]}))
        else:
            sub_pairs[self.reason_col_name] = sub_pairs[self.cname].apply(lambda x: reason % str(x))
        return sub_pairs


class global_filter(object):
    def __init__(self, loc_feat):
        self.loc_feat = loc_feat

    def filtering(self, key_column, percentile=0.2, mode='gt'):
        val = self.loc_feat[key_column].quantile(q=percentile)
        if mode == 'gt':
            sub_loc = self.loc_feat.loc[self.loc_feat[key_column] >= val,:]
        else:
            sub_loc = self.loc_feat.loc[self.loc_feat[key_column] <= val,:]

        self.loc_feat = sub_loc.reset_index(drop=True)
        return self

    def city_filter(self, city_name, key_column='city'):
        self.loc_feat = self.loc_feat[self.loc_feat[key_column] == city_name].reset_index(drop=True)
        return self

    def exfiltering(self, loc_feat, key_column, percentile=0.2, mode='gt'):
        val = loc_feat[key_column].quantile(q=percentile)
        if mode == 'gt':
            sub_loc = self.loc_feat.loc[self.loc_feat[key_column] >= val,:]
        else:
            sub_loc = self.loc_feat.loc[self.loc_feat[key_column] <= val,:]

        return sub_loc.reset_index(drop=True)

    def end(self):
        return self.loc_feat


class sub_rec_condition(object):
    def __init__(self, loc_feat,bid='atlas_location_uuid'):
        """
        comp_loc: company-location affinities of a certain city
        cond_col = column of location used for filtering
        """
        self.loc_feat = loc_feat
        self.cond_col = []
        self.reason = []
        self.bid = bid

    def filtering(self, cond_col, percentile=0.5, reason='many things'):
        self.cond_col.append(cond_col)
        val = self.loc_feat[cond_col].quantile(q=percentile)
        if max(val, 10):
            self.loc_feat = self.loc_feat.loc[self.loc_feat[cond_col] >= val,:].reset_index(drop=True)
            self.reason.append(reason)
        return self

    def exfiltering(self, cond_col, percentile=0.6, reason='many things', reason_col_name='reason'):
        self.cond_col.append(cond_col)
        val = self.loc_feat[cond_col].quantile(q=percentile)
        if max(val, 10):
            sub_loc = self.loc_feat.loc[self.loc_feat[cond_col] >= val,:].reset_index(drop=True)
        sub_loc[reason_col_name] = reason
        return sub_loc[[self.bid, reason_col_name]]

    def end(self):
        return self.loc_feat


# ======================================================================================================================
def ab(df,sep=','):
    return sep.join(df.values)


def merge_rec_reason_rowise(sub_pairs, group_cols: list, merge_col: str, sep=','):
    return sub_pairs.groupby(group_cols)[merge_col].apply(ab,sep=sep).reset_index()


def merge_rec_reason_colwise(sub_pairs, cols=['reason1', 'reason2'], dst_col='reason', sep=','):
    sub_pairs[dst_col] = sub_pairs[cols[0]].str.cat(sub_pairs[cols[1]], sep=sep)
    return sub_pairs


def merge_str_2_json_rowise(row, src_cols:list, jsKey = 'reasons'):
    """
    row in dataframe
    output json style string
    """
    jsRs = {}
    jsRs[jsKey] = []
    for src_col in src_cols:
        if str(row[src_col]) != '':
            jsRs[jsKey].append(str(row[src_col]))

    return json.dumps(jsRs)



def merge_str_2_json_rowise_reformat(row, src_cols:list, jsKey = 'reasons',target_phss=[]):
    """
    row in dataframe
    output json style string
    """
    jsRs = {}
    jsRs[jsKey] = []
    nreason = []
    for src_col in src_cols:
        rs = str(row[src_col])
        if rs:
            need_reformat = False
            for target_phs in target_phss:
                if rs.startswith(target_phs):
                    need_reformat = True
                    replace_phs = target_phs

            if need_reformat:
                rs = rs.replace(replace_phs, '')
                multi_row_reason = rs.rstrip('.').split('. ')
                multi_row_reason = [c + '.' for c in multi_row_reason]
                nreason = nreason + multi_row_reason
            else:
                nreason.append(rs)
    jsRs[jsKey] = nreason
    return json.dumps(jsRs)




# ======================================================================================================================

def list2json(x, sep=','):
    x = str(x)
    k = ''
    ltx = x.split(sep)
    for item in ltx:
        if k != '':
            if item != '':
                k = k + ',' + "\"" + item + "\""
            else:
                pass
        else:
            if item != '':
                k = "\"" + item + "\""
            else:
                pass
    k = '[' + k + ']'
    return k


def reason_json_format(df, col_name: str = 'reason', sep=','):
    df[col_name] = df[col_name].apply(lambda x: '{\"reasons\":' + list2json(x, sep) + '}')
    return df


# ======================================================================================================================
# ======================================================================================================================
# get_dl_sub_recommend_reason
# ======================================================================================================================
# ======================================================================================================================
class featsrc(Enum):
    company = 0
    location = 1
    region = 2


class feature_translate_of_locaiton_similar_in(object):
    def __init__(self,tail_delimiter=''):
        self.col2phs = {}
        self.init_dict()

        if tail_delimiter != '':
            for key,item in self.col2phs():
                self.col2phs[key] = item + tail_delimiter

        self.keytuple = [key for key in self.col2phs.keys() if isinstance(key, tuple)]


    def getItem(self, gvkey):
        #tuple matching first

        for key in self.keytuple:
            if gvkey in key:
                return {'status': True,
                    'key': gvkey,
                    'item': self.col2phs[key]}
        # precision matching
        if gvkey in self.col2phs.keys():
            return {'status': True,
                    'key': gvkey,
                    'item': self.col2phs[gvkey]}

        return {'status': False}


    def init_dict(self):
        self.col2phs['score_predicted_eo'] = (
            featsrc.location, 'The predicted economic occupancy is as high as your current location')
        self.col2phs['score_employer'] = (
            featsrc.location, 'There are as many good businesses in this location as your current location')
        self.col2phs['building_class'] = (
            featsrc.location, 'The building class in this location is as good as your current one')
        self.col2phs['num_retail_stores'] = (
            featsrc.location, 'There are enough retail stores in this region as your current one')
        self.col2phs['num_doctor_offices'] = (
            featsrc.location, 'The medical service in this location is as good as your current location')
        self.col2phs[('num_eating_places', 'num_drinking_places')] = (
            featsrc.location, 'Eating and drinking are as convenient as your current location')
        self.col2phs['num_hotels'] = (
            featsrc.location, 'This location has as many hotels to host your visitors as your current location')
        self.col2phs['num_fitness_gyms'] = (
            featsrc.location,
            'This location has as many gyms as your current location to take care of the health of your employee')
        self.col2phs['population_density'] = (
            featsrc.location,
            'The demographics, especially the population density, in this location is similar to your current location, and will meet your hiring needs')
        self.col2phs['pct_female_population'] = (
            featsrc.location,
            'The gender diversity of this location is as good as your current location')
        self.col2phs['median_age'] = (
            featsrc.location,
            'The demographics, especially the median age, in this location is similar to your current location, and will meet your hiring needs')
        self.col2phs['income_per_capita'] = (
            featsrc.location,
            'The employee statistics, especially the income per capita, in this location is similar to your current location, and will meet your hiring needs')
        self.col2phs['walk_score'] = (
            featsrc.location,
            'This location is as easily accessible by walk as your current location')
        self.col2phs['bike_score'] = (
            featsrc.location,
            'This location is as easily accessible by bike as your current location')
        self.col2phs['num_emp_weworkcore'] = (
            featsrc.location,
            'The employee statistics, especially the number of employees in the core industry, in this location is similar to your current location, and will meet your hiring needs')
        self.col2phs['num_poi_weworkcore'] = (
            featsrc.location,
            'The business environment, especially the number of core businesses relevant to your company, in this area is similar to your current location, and will meet your business development needs')
        self.col2phs['pct_wwcore_business'] = (
            featsrc.location,
            'The business environment, especially the main category of surrounding businesses, in this area is similar to your current location, and will meet your business development needs')
        self.col2phs['pct_wwcore_employee'] = (
            featsrc.location,
            'The business environment, especially the percentage of employees of core categories relevant to your company, in this area is similar to your current location, and will meet your business development needs')




class feature_translate(object):
    def __init__(self):
        self.col2phs = {}
        self.init_dict()

    def init_dict(self):
        # company
        self.col2phs['emp_here'] = (
        featsrc.company, 'This location matches the amount of employees your company plans to hire locally')
        # self.col2phs['emp_here_range'] = (featsrc.company, 'local employee number') avoid for duplicating reason, leave it to dummy category
        self.col2phs['emp_total'] = (
        featsrc.company, 'Companies with similar size as yours also has an office in this location')
        self.col2phs['sales_volume_us'] = (featsrc.company, 'This location can supply the sales volume of the company')
        self.col2phs['location_type'] = (
        featsrc.company, 'This location can provide the office type demanded by your company')
        self.col2phs['square_footage'] = (
        featsrc.company, 'This location can match the expected square footage of your company')
        self.col2phs['primary_sic_2'] = (
        featsrc.company, 'This location is good for the industry type of your business')
        # building
        self.col2phs['score_predicted_eo'] = (featsrc.location, 'a high predicted score of economic occupancy')
        self.col2phs['building_class'] = (featsrc.location, 'a high quality of facilities')
        # region
        self.col2phs['num_retail_stores'] = (featsrc.region, 'Shopping amenities is enough for your company')
        self.col2phs['num_doctor_offices'] = (featsrc.region, 'Health amenities is enough for your company')
        self.col2phs['num_eating_places'] = (featsrc.region, 'Eating amenities is enough for your company')
        self.col2phs['num_drinking_places'] = (featsrc.region, 'Relaxing amenities is enough for your company')
        self.col2phs['num_hotels'] = (featsrc.region, 'Hotel amenities is enough for your company')
        self.col2phs['num_fitness_gyms'] = (featsrc.region, 'Gym amenities is enough for your company')
        self.col2phs['population_density'] = (featsrc.region, 'Population density is suitable for your company')
        self.col2phs['pct_female_population'] = (featsrc.region, 'Gender diversity is suitable for your company')
        self.col2phs['median_age'] = (featsrc.region, 'Age distribution of the population meets the need of your company')
        self.col2phs['income_per_capita'] = (featsrc.region, 'Income level is adequate to your company')
        self.col2phs['pct_masters_degree'] = (featsrc.region, 'Education degree meets the need of your company')
        self.col2phs['walk_score'] = (featsrc.region, 'It is walking friendly')
        self.col2phs['bike_score'] = (featsrc.region, 'It is biking friendly')

    def getItem(self, gvkey):
        # precision matching
        if gvkey in self.col2phs.keys():
            return {'status': True,
                    'key': gvkey,
                    'item': self.col2phs[gvkey]}
        # rough matching
        for key in self.col2phs.keys():
            if gvkey.startswith(key):
                return {'status': True,
                        'key': key,
                        'item': self.col2phs[key]}

        return {'status': False}

    def merge_lst(self, lst: list, pre_phs='', post_phs='', sep=', '):
        phs = ''
        #         print(lst)
        for c in lst:
            phs = phs + c + sep
        if lst:
            phs = phs[:-2]  # get rid of last ', '
        # print(phs)
        if pre_phs:
            pre_phs = pre_phs + ' '
        if post_phs:
            post_phs = ' ' + post_phs
        return pre_phs + phs + post_phs

    def merge_phs(self, lst: list, sep='; '):
        phs = ''
        _lst = [p for p in lst if p]
        for p in _lst:
            if phs:
                phs = phs + sep + p
            else:
                phs = p
        return phs

    def make_sense(self, input_lst):
        if isinstance(input_lst, list):
            pass
        elif isinstance(input_lst, str):
            input_lst = input_lst.replace('[', '', 1)
            input_lst = input_lst.replace(']', '', 1)
            input_lst = [e for e in input_lst.split(',') if e]
        else:
            return 'Err:input type'

        # print(len(input_lst))
        # in case of irrelavant data
        # input_lst = [self.col2phs[key] for key in input_lst if key in self.col2phs.keys()]

        comp_lst, loc_lst, region_lst = [], [], []

        key_lst = []
        for key in input_lst:
            #             print(key)
            ret = self.getItem(key)

            if ret['status']:
                if ret['key'] not in key_lst:  # get rid of the duplicate feature
                    key_lst.append(ret['key'])
                    phss = ret['item']
                    if phss[0] == featsrc.company:
                        comp_lst.append(phss[1])
                    elif phss[0] == featsrc.location:
                        loc_lst.append(phss[1])
                    elif phss[0] == featsrc.region:
                        region_lst.append(phss[1])

                        #         print(comp_lst,loc_lst,region_lst)

        if comp_lst:  # not empty assert
            comp_phs = self.merge_lst(comp_lst, pre_phs='', post_phs='', sep = '. ')
        else:
            comp_phs = ''

        if loc_lst:
            loc_phs = self.merge_lst(loc_lst, pre_phs='This location has', post_phs='', sep='. ')
        else:
            loc_phs = ''

        if region_lst:
            region_phs = self.merge_lst(region_lst, pre_phs='', post_phs='inside the region', sep = '. ')
        else:
            region_phs = ''

        # print(comp_phs,loc_phs,region_phs)
        final_phs = self.merge_phs([comp_phs, loc_phs, region_phs],sep='. ')
        if final_phs:
            return 'Implicit reason: ' + final_phs + '.'
        else:
            return ''

    def make_sense_json(self, input_lst, jsKey='A'):
        last_delimeter = '.'
        if isinstance(input_lst, list):
            pass
        elif isinstance(input_lst, str):
            input_lst = input_lst.replace('[', '', 1)
            input_lst = input_lst.replace(']', '', 1)
            input_lst = [e for e in input_lst.split(',') if e]
        else:
            return 'Err:input type'

        comp_lst, loc_lst, region_lst = [], [], []

        key_lst = []
        for key in input_lst:
            ret = self.getItem(key)

            if ret['status']:
                if ret['key'] not in key_lst:  # get rid of the duplicate feature
                    key_lst.append(ret['key'])
                    phss = ret['item']
                    if phss[0] == featsrc.company:
                        comp_lst.append(phss[1])
                    elif phss[0] == featsrc.location:
                        loc_lst.append(phss[1])
                    elif phss[0] == featsrc.region:
                        region_lst.append(phss[1])

        comp_phs = [phs+last_delimeter for phs in comp_lst if phs]
        loc_phs = [ 'This location has ' + phs + last_delimeter for phs in loc_lst if phs]
        region_phs = [ phs + ' inside the region' + last_delimeter for phs in region_lst if phs]

        final_js = {jsKey: (comp_phs+loc_phs+region_phs) }
        return final_js


# =======================================================================================================================
# =======================================================================================================================
# get_sub_recommend_reason_after_similarity
# compare the recommended location and current location and find which feature is similar between them.
# =======================================================================================================================
# =======================================================================================================================
class sub_rec_similar_location(object):
    """
    In which feature, those tow locations are similar with each other.
    """

    def __init__(self, cont_col_name, dummy_col_name, reason_col_name='reason',cid='duns_number',bid='atlas_location_uuid'):
        self.cont_col_name = cont_col_name
        self.dummy_col_name = dummy_col_name
        self.reason_col_name = reason_col_name
        self._info = 'It will keep index of sspd'
        self.threshold = 0.03
        self.reason_translator = feature_translate_of_locaiton_similar_in()
        self.cid = cid
        self.bid = bid

    def get_reason(self, sspd, comp_loc, loc_feat, reason='Location similar in: ', multi_flag=False):
        loc_comp_loc = sspd.merge(comp_loc, how='inner', on=self.cid, suffixes=['', '_grd']) \
            [[self.bid, self.cid, self.bid+'_grd']]

        loc_comp_loc = loc_comp_loc.merge(loc_feat, on=self.bid, suffixes=['', '_pred'])
        loc_comp_loc = loc_comp_loc.merge(loc_feat, left_on=self.bid+'_grd', right_on=self.bid,
                                          suffixes=['', '_grd'])

        if self.reason_col_name not in loc_comp_loc.columns:
            loc_comp_loc[self.reason_col_name] = ''

        for c in self.dummy_col_name:
            ret_reason = self.reason_translator.getItem(gvkey=c)
            if ret_reason['status']:
                ca = c + '_grd'
                tmp = loc_comp_loc[[self.bid, self.cid, c, ca]].dropna()
                tmp = tmp.loc[tmp[c] == tmp[ca],:]

                tmp['reason'] = ret_reason['item'][1]
                # tmp['reason'] = tmp['reason']
                loc_comp_loc[[self.reason_col_name]] = \
                    loc_comp_loc[self.reason_col_name].str.cat(tmp['reason'], join='left', sep='|', na_rep='')
        for c in self.cont_col_name:
            ret_reason = self.reason_translator.getItem(gvkey=c)
            if ret_reason['status']:
                ca = c + '_grd'
                tmp = loc_comp_loc[[self.bid, self.cid, c, ca]].dropna()
                tmp = tmp.loc[abs(tmp[c] - tmp[ca]) / (tmp[ca] + 1e-5) < self.threshold,:]
                tmp['reason'] = ret_reason['item'][1]
                loc_comp_loc[[self.reason_col_name]] = \
                    loc_comp_loc[self.reason_col_name].str.cat(tmp['reason'], join='left', sep='|', na_rep='')

        def clean(text):  # problem caused by str.cat. Thus clean is a must.
            clean_str = '. '.join([c for c in text.split('|') if c != ''])
            return clean_str

        def cnter(text):
            ns = text.count('.')
            return ns

        loc_comp_loc[self.reason_col_name] = loc_comp_loc[self.reason_col_name].apply(lambda text: clean(text))

        if multi_flag:  # for region based model
            loc_comp_loc['cnt'] = loc_comp_loc[self.reason_col_name].apply(lambda text: cnter(text))
            loc_comp_loc['cnt'] = loc_comp_loc['cnt'].fillna(0)

            idx = loc_comp_loc.groupby([self.bid, self.cid])['cnt'].idxmax()
            loc_comp_loc = (loc_comp_loc.loc[idx]).reset_index()

        loc_comp_loc = loc_comp_loc[[self.bid, self.cid , self.reason_col_name]]
        loc_comp_loc = loc_comp_loc[loc_comp_loc[self.reason_col_name] != '']
        loc_comp_loc[[self.reason_col_name]] = reason + loc_comp_loc[self.reason_col_name] + '.'
        return loc_comp_loc


class sub_rec_similar_company_v2(object):
    """
    Retrieve the name of similar company inside the recommended location
    """

    def __init__(self, comp_loc, sspd, thresh=0.05,cid='duns_number',bid='atlas_location_uuid'):
        self._gr_dat = comp_loc
        self._pred_dat = sspd
        self._sim_thresh = thresh
        self.cid = cid
        self.bid = bid

    def get_reason_batch(self, comp_feat, comp_feat_col, comp_feat_normed, reason_col_name, batch_size = 10000):
        cid = self.cid
        bid = self.bid
        gr_dat = self._gr_dat

        batch_iter = ceil(1.0*len(self._pred_dat)/batch_size)
        total_result = []
        pbar = tqdm.tqdm(total = len(self._pred_dat))

        for i in range(batch_iter):
            # print('processing %d of %d'%(i,batch_iter) )
            bgidx = i*batch_size
            edidx = min( (i+1)*batch_size, len(self._pred_dat) )
            pbar.update(edidx-bgidx)
            pred_dat = self._pred_dat.iloc[bgidx:edidx]

            pred_gr_dat = pred_dat.merge(gr_dat[[bid, cid]], on=[bid],
                                         how='left', suffixes=['_prd', '_grd'])

            prd_comp_feat = \
                pred_gr_dat[[cid+'_prd']].rename(columns={cid + '_prd': cid}).merge(comp_feat_normed,
                                                                             on=cid,
                                                                             how='left')[
                    comp_feat_col].to_numpy()
            grd_comp_feat = \
                pred_gr_dat[[cid+'_grd']].rename(columns={cid + '_grd': cid}).merge(comp_feat_normed,
                                                                             on=cid,
                                                                             how='left')[
                    comp_feat_col].to_numpy()

            prd_comp_feat = normalize(prd_comp_feat, axis=1)
            grd_comp_feat = normalize(grd_comp_feat, axis=1)
            dist = 1 - (prd_comp_feat * grd_comp_feat).sum(axis=1).reshape(-1, 1)

            distpd = pd.DataFrame(dist, columns=['dist'])

            pred_gr_dat2 = pd.concat([pred_gr_dat[[bid, cid+'_prd', cid+'_grd']], distpd],
                                     axis=1)
            pred_gr_dat2.loc[pred_gr_dat2['dist'] < 1e-12, 'dist'] = 1
            result = pred_gr_dat2.loc[
                pred_gr_dat2.groupby([bid, cid+'_prd'])['dist'].idxmin()].reset_index(drop=True)

            result = result.loc[result['dist'] <= self._sim_thresh,:]

            result = \
                result.merge(comp_feat[[cid, 'business_name']], left_on=cid+'_grd', right_on=cid,
                             how='left',
                             suffixes=['', '_useless'])[[bid, cid+'_prd', 'business_name', 'dist']]
            result = result.rename(columns={'business_name': reason_col_name, cid+'_prd': cid})

            result['dist'] = result['dist'].round(4)
            result[reason_col_name] = 'There is a similar company already inside: ' + result[
                reason_col_name] + ' with diff: ' + result[
                                          'dist'].astype(str) + '. '
            total_result.append(result)

        result = pd.concat(total_result,axis=0,sort=False)
        pbar.close()
        # print('pairs %d' % len(result))
        return result

    def get_reason(self, comp_feat, comp_feat_col, comp_feat_normed, reason_col_name):
        gr_dat = self._gr_dat
        pred_dat = self._pred_dat
        cid = self.cid
        bid = self.bid

        pred_gr_dat = pred_dat.merge(gr_dat[[bid, cid]], on=[bid],
                                     how='left', suffixes=['_prd', '_grd'])
        # print('pairs to be calced:%d' % len(pred_gr_dat))

        prd_comp_feat = \
            pred_gr_dat[[cid+'_prd']].rename(columns={cid+'_prd': cid}).merge(comp_feat_normed,
                                                                                                      on=cid,
                                                                                                      how='left')[
                comp_feat_col].to_numpy()
        grd_comp_feat = \
            pred_gr_dat[[cid+'_grd']].rename(columns={cid+'_grd': cid}).merge(comp_feat_normed,
                                                                                                      on=cid,
                                                                                                      how='left')[
                comp_feat_col].to_numpy()

        prd_comp_feat = normalize(prd_comp_feat, axis=1)
        grd_comp_feat = normalize(grd_comp_feat, axis=1)
        dist = 1 - (prd_comp_feat * grd_comp_feat).sum(axis=1).reshape(-1, 1)

        distpd = pd.DataFrame(dist, columns=['dist'])

        pred_gr_dat2 = pd.concat([pred_gr_dat[[bid, cid+'_prd', cid+'_grd']], distpd],
                                 axis=1)
        pred_gr_dat2.loc[pred_gr_dat2['dist'] < 1e-12, 'dist'] = 1
        result = pred_gr_dat2.loc[
            pred_gr_dat2.groupby([bid, cid+'_prd'])['dist'].idxmin()].reset_index(drop=True)

        result = result.loc[result['dist'] <= self._sim_thresh,:]

        result = \
            result.merge(comp_feat[[cid, 'business_name']], left_on=cid+'_grd', right_on=cid,
                         how='left',
                         suffixes=['', '_useless'])[[bid, cid+'_prd', 'business_name', 'dist']]
        result = result.rename(columns={'business_name': reason_col_name, cid+'_prd': cid})

        result['dist'] = result['dist'].round(4)
        result[reason_col_name] = 'There is a similar company already inside: ' + result[reason_col_name] + ' with diff: ' + result[
            'dist'].astype(str) + '. '
        # print('pairs %d' % len(result))
        return result


def geo_distance(lng1, lat1, lng2, lat2):
    lng1, lat1, lng2, lat2 = map(radians, [lng1, lat1, lng2, lat2])
    dlon = lng2 - lng1
    dlat = lat2 - lat1
    a = sin(dlat / 2) ** 2 + cos(lat1) * cos(lat2) * sin(dlon / 2) ** 2
    dis = 2 * asin(sqrt(a)) * 6371 * 1000
    return dis


class sub_rec_location_distance(object):
    """
    If the recommended location is close to the current location(distance <= dist_thresh),
    it will be considered as a recommendation reason.
    """

    def __init__(self, reason_col_name='reason',cid = 'duns_number',bid='atlas_location_uuid'):
        self.reason_col_name = reason_col_name
        self.threshold = 0.03
        self.cid = cid
        self.bid = bid

    def get_reason(self, sspd, loc_feat, comp_feat, dist_thresh=3.2e3):
        # loc_comp_loc = sspd.merge(comp_loc, how='inner', on='duns_number', suffixes=['', '_grd']) \
        #     [['atlas_location_uuid', 'duns_number', 'atlas_location_uuid_grd']]

        cid = self.cid
        bid = self.bid

        rt_key_col = [bid, 'latitude', 'longitude']
        loc_comp_loc = sspd[[bid, cid]].merge(loc_feat[rt_key_col],
                                                                          on=bid,
                                                                          suffixes=['', '_pred'])
        rt_key_col = [cid, 'latitude', 'longitude']
        loc_comp_loc = loc_comp_loc.merge(comp_feat[rt_key_col], on=cid, suffixes=['', '_grd'])
        loc_comp_loc['geo_dist'] = loc_comp_loc.apply(
            lambda row:
            geo_distance(row['longitude'], row['latitude'],
                         row['longitude_grd'], row['latitude_grd']), axis=1)
        loc_comp_loc = loc_comp_loc.loc[loc_comp_loc['geo_dist'] <= dist_thresh,:]
        loc_comp_loc[self.reason_col_name] = 'Recommended location is close to current location(<' + str(round(dist_thresh / 1e3, 1)) + 'km). '
        return loc_comp_loc[[bid, cid, self.reason_col_name]]

## Inventory bom
class sub_rec_inventory_bom(object):
    def __init__(self, invdb, reason='Inventory reason: This available space of this location can hold your company.',bid='atlas_location_uuid',cid='duns_number'):
        self.invdb = invdb.sort_values([bid, 'report_month']) \
            .drop_duplicates([bid], keep='last')
        self.reason = reason
        self.bid = bid
        self.cid = cid

    def get_reason(self, sspd, comp_feat, comp_col='emp_here', inv_col='sum_reservable_office_capacity',
                   reason_col='inventory'):
        bid = self.bid
        cid = self.cid
        sfx = ['','_right']
        clpair = sspd[[bid, cid]]
        comp_feat = comp_feat[[cid,comp_col]]
        clpair = clpair.merge(comp_feat[[cid, comp_col]].fillna(0), on=cid, suffixes=sfx, how='left').merge(self.invdb, on=bid,
                                                                                                  suffixes=sfx)
        clpair = clpair.fillna(0)
        clpair[reason_col] = clpair.apply(lambda x: self.reason if int(x[comp_col]) <= int(x[inv_col]) else '', axis=1)
        return clpair[[cid, bid, reason_col]]

## CompStak
def translate_compstak_date_and_format(exp_date: str, cur_date, reason):
    exp_date = exp_date.replace(' ', '')[:10]

    try:
        exp_date = datetime.datetime.strptime(str(exp_date), "%Y-%m-%d")
    except:
        exp_date = '0001-01-01'
        exp_date = datetime.datetime.strptime(str(exp_date), "%Y-%m-%d")

    diff_date = exp_date - cur_date
    diff_month = ceil(diff_date.days / 28)

    # trans_reason = reason.replace('XXX', str(diff_month))
    trans_reason = reason%int(diff_month)

    if diff_month > 0:
        return trans_reason
    else:
        return ''

def translate_compstak_date(exp_date:str,cur_date):
    exp_date = exp_date.replace(' ', '')[:10]
    try:
        exp_date = datetime.datetime.strptime(str(exp_date), "%Y-%m-%d")
    except:
        exp_date = '0001-01-01'
        exp_date = datetime.datetime.strptime(str(exp_date), "%Y-%m-%d")

    diff_date = exp_date - cur_date
    diff_month = ceil(diff_date.days / 28)

    return diff_month

class sub_rec_compstak(object):
    def __init__(self, cpstkdb, cpstkdnb,
                 reason='Compstack reason: The lease will expire in %d months.',
                 bid='altas_location_uuid',
                 cid='duns_number'):
        sfx = ['', '_right']
        cpstkdb = cpstkdb[['tenant_id', 'expiration_date']]
        cpstkdnb = cpstkdnb[[cid, 'tenant_id']]
        self.db = cpstkdnb.merge(cpstkdb, on='tenant_id', suffixes=sfx)
        self.db['expiration_date'] = self.db['expiration_date'].fillna('0001-01-01')
        self.db = self.db[[cid, 'expiration_date']]
        self.reason = reason
        self.cid = cid
        self.bid = bid

    def get_reason(self, sspd, reason_col='compstak'):
        bid = self.bid
        cid = self.cid
        sfx = ['', '_right']
        clpair = sspd[[bid, cid]]
        cur_date = datetime.datetime.now()
        self.db['month_remain'] = self.db['expiration_date'].apply(
            lambda x: translate_compstak_date(str(x), cur_date)
        )

        self.db['month_remain'] = self.db['month_remain'].astype(int)

        self.db = self.db.sort_values([cid, 'month_remain']) \
            .drop_duplicates([cid], keep='last')

        self.db[reason_col] = self.db['month_remain'].apply(
            lambda x: self.reason%int(x) if x > 0 else ''
        )


        clpair = clpair.merge(self.db[[cid, reason_col]], on=cid, suffixes=sfx)
        return clpair[[cid, bid, reason_col]]
