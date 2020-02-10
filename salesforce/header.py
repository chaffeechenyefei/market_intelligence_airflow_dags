import os
from enum import Enum

pj = os.path.join


inventory_file = 'inventory_bom.csv'
compstak_file = 'tetris_mv_tetris_transactions_2016_current.csv'
demand_file = 'demand_signals_191110.csv'
salesforce_file = 'salesforce/opportunities.csv'
salesforce_pos_file = 'salesforce/salesforce_pair.csv'
salesforce_opp_x_atlas_file = 'salesforce/salesforce_opp_x_atlas.csv'

reason_support_hot_location = 'salesforce/reason/hot_locations_0121.csv'
reason_support_item2item = 'salesforce/reason/item2item.csv'

mid_salesforce_similairty_file = 'salesforce_pair.csv'

# compstak_dnb_match_file = 'relation_dnb_compstak_0120.csv'
# salesforce_dnb_file = 'salesforce_comp_city_from_opp.csv'
# salesforce_dnb_info_file = 'salesforce_acc_duns_info.csv'
# salesforce_dnb_match_file = 'relation_dnb_account_0128.csv'

class rsKey(Enum):
    Additional = 'Additional Reasons'
    Demand = 'Demand Signals'
    Portfolio = 'Portfolio signals'


hdargs = {
    "run_root":"/home/ubuntu/location_recommender_system",
    "ls_card":"location_scorecard_200106.csv",
    "reason_col_name":{
        "reason_salesforce_hot_location":
            {"p":0,"useFLG":1,"cache":1,"rsKey":rsKey.Additional.value},
        "reason_salesforce_similar_location":
            {"p":1,"useFLG":1,"cache":1,"rsKey":rsKey.Additional.value},
        "reason_salesforce_demand_x_inventory":
            {"p":2,"useFLG":1,"cache":1,"rsKey":rsKey.Demand.value},
    },

    "bid":"atlas_location_uuid",
    "fid":"account_id",

    "test_db":"salesforce_tmp_table",
    "dev_db":"salesforce_reason_table",
    "test":0, #whether using test_db(space/folder) or dev_db
    "final_file_name":"result/salesforce_recommendation_reason",
}


datapath = hdargs['run_root']
TEST_FLG = hdargs["test"]

if TEST_FLG:
    datapath_mid = pj(datapath, hdargs["test_db"])
    salesforcedbname = hdargs["test_db"]
else:
    datapath_mid = pj(datapath, hdargs["dev_db"])
    salesforcedbname = hdargs["dev_db"]
