import pandas as pd
import numpy as np

from dnb.data_loader import data_process
import os

pj = os.path.join
sfx = ['','_right']

bid = 'atlas_location_uuid'
fid = 'account_id'

class salesforce_pair(object):
    """
    generate [salesforce_pair] from [opportunities]
    """
    def __init__(self, datapath, opp_file, lscard_file):
        self.datapath = datapath
        self.opp_file = opp_file
        self.lscard_file = lscard_file
        self.bid = bid
        self.fid = fid

    def generate(self , savename='salesforce/salesforce_opp_x_atlas.csv'):
        datapath = self.datapath
        bid = self.bid
        fid = self.fid

        dtld = data_process(root_path=datapath)
        city = dtld.ls_col['city']

        origin_opp_file = self.opp_file
        lscardfile = self.lscard_file

        opp_pos_pair_file = savename
        savepath = pj(datapath, savename)

        opp_pos_pair = dtld.load_opportunity(db='', dbname=origin_opp_file, save_dbname=opp_pos_pair_file)
        print('opp_pos_pair:%d' % len(opp_pos_pair))
        lscard = dtld.load_location_scorecard_msa(db='', dbname=lscardfile, is_wework=True)

        opp_pos_city = opp_pos_pair[[bid, fid]].merge(lscard, on=bid, suffixes=sfx)[[fid, city]]
        opp_pos_city = opp_pos_city.drop_duplicates([fid, city], keep='last')
        print('opp_pos_city:%d' % len(opp_pos_city))

        opp_atlas = opp_pos_city.merge(lscard[[bid, city]], on=city, suffixes=sfx)[[fid, bid, city]]

        opp_atlas.to_csv(savepath)
        print('%d opp_x_atlas saved.' % len(opp_atlas))
        return opp_atlas
























