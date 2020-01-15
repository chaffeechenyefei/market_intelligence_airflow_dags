from dnb.header import *
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
