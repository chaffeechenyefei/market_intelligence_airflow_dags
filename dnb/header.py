import os
from dnb.data_loader import data_process

pj = os.path.join

feature_column = {
    'not_feat_col': ['duns_number',
                     'atlas_location_uuid',
                     'longitude_loc',
                     'latitude_loc',
                     'city',
                     'label'],
    'cont_col_nameC': ['emp_here', 'emp_total', 'sales_volume_us', 'square_footage', 'emp_here_range'],
    'spec_col_nameC': 'emp_here_range',
    'cont_col_nameL': ['score_predicted_eo', 'score_employer', 'num_emp_weworkcore', 'num_poi_weworkcore',
                       'pct_wwcore_employee', 'pct_wwcore_business', 'num_retail_stores', 'num_doctor_offices',
                       'num_eating_places', 'num_drinking_places', 'num_hotels', 'num_fitness_gyms',
                       'population_density', 'pct_female_population', 'median_age', 'income_per_capita',
                       'pct_masters_degree', 'walk_score', 'bike_score'],
    'key_col_comp': ['duns_number'],
    'key_col_loc': ['atlas_location_uuid'],

    'dummy_col_nameL': ['building_class'],
    'dummy_col_nameC': ['major_industry_category', 'location_type', 'primary_sic_2_digit'],
}


cityabbr = ['PA', 'SF', 'SJ', 'LA', 'NY']
citylongname = ['Palo Alto','San Francisco','San Jose','Los Angeles', 'New York']

origin_comp_file = ['dnb_pa.csv', 'dnb_sf.csv', 'dnb_sj.csv', 'dnb_Los_Angeles.csv', 'dnb_New_York.csv']

inventory_file = 'inventory_bom.csv'
compstak_file = 'tetris_mv_tetris_transactions_2016_current.csv'
compstak_dnb_match_file = 'relation_dnb_compstak_0120.csv'
salesforce_dnb_file = 'salesforce_comp_city_from_opp.csv'
salesforce_dnb_info_file = 'salesforce_acc_duns_info.csv'
salesforce_dnb_match_file = 'relation_dnb_account_0128.csv'
demand_file = 'demand_signals_191110.csv'

kwargs_key = 'task_instance'

#in the future it can be moved into Variables
hdargs = {
    "run_root":"/home/ubuntu/location_recommender_system",
    "ls_card":"location_scorecard_200106.csv",
    "apps":"_200106.csv",
    "otversion":"_200106.csv",
    "jsonFLG":1,
    "reason_col_name":{
        "reason_similar_biz":
            {"p":4,"useFLG":0,"cache":1,"rsKey":"Additional Reasons"},
        "reason_location_based":
            {"p":8,"useFLG":0,"cache":1, "rsKey":"Additional Reasons"},
        "reason_model_based":
            {"p":10,"useFLG":0,"cache":1,"rsKey":"Additional Reasons"},
        "reason_similar_location":
            {"p":9,"useFLG":0,"cache":1,"rsKey":"Additional Reasons"},
        "reason_similar_company":
            {"p":6,"useFLG":0,"cache":1,"rsKey":"Additional Reasons"},
        "reason_close_2_current_location":
            {"p":5,"useFLG":0,"cache":1,"rsKey":"Portfolio signal"},
        "reason_inventory_bom":
            {"p": 3, "useFLG": 0,"cache":1,"rsKey":"Portfolio signal"},
        "reason_compstak":
            {"p": 1, "useFLG": 0,"cache":1,"rsKey":"Portfolio signal"},
        "reason_talent_score":
            {"p":7, "useFLG":1 ,"cache":1,"rsKey":"Additional Reasons"},
        "reason_price_based":
            {"p":2, "useFLG":0,"cache":1,"rsKey":"Portfolio signal"},
        "reason_demand_x_inventory":
            {"p":0, "useFLG":0 , "cache":1,"rsKey":"Demand Signals"},
    },
    "filter_col_name":{#'column name':'explanation'
        "filter_price":"The price per desk lower than current effective rent.",
        "filter_size_dnb":"The team size from dnb should be smaller than max_capacity of the building.",
        "filter_size_demand":"The team size from demand should be smaller than max_capacity of the building.",
        "filter_move_in_date":"filter_move_in_date",
    },
    "cid":"duns_number",
    "bid":"atlas_location_uuid",

    "geo_bit":7,
    "dist_thresh":500,

    "test_db":"tmp_table",
    "dev_db":"reason_table",
    "test":0, #whether using test_db(space/folder) or dev_db
    "final_file_name":"result/sub_all_similarity_multi",

    "do_data_split":0, #whether doing data split
    "ratio_data_split":0.9,
    "train_file":"region_train",
    "test_file":"region_test",
    "maxK_region":80,
    "test_round":8,
    "use_additional_feat":1,

    "dnb_dnn_program_path":"/home/ubuntu/mygit/locationIntelligenceModel/",
    "dnb_dnn_normalization_path":"/home/ubuntu/mygit/locationIntelligencePipeline/",
    "dnb_dnn_prediction_exe":"main_location_intelligence_region.py",
    "dnb_dnn_cmd":{
        "run_root":"result/location_RSRBv5dev_191213",
        "model":"location_recommend_region_model_v5",
        "lr":0.01,
    },
    "dnb_dnn_embedding_exe":"get_embedding_feature_region.py",
    "dnb_dnn_model_based_exe":"main_location_intelligence_region_based_reason.py",
    "dnb_data_normalizaiton_with_param_exe":"step003_get_csv_of_normalized_data_additonaly_ww.py"
}


datapath = hdargs['run_root']

TEST_FLG = hdargs["test"]
if TEST_FLG:
    datapath_mid = pj(datapath, hdargs["test_db"])
    dnbdbname = hdargs["test_db"]
else:
    datapath_mid = pj(datapath, hdargs["dev_db"])
    dnbdbname = hdargs["dev_db"]

"""
If using additional_feat, data should be loaded via db.
"""
if hdargs["use_additional_feat"]:
    dataloader = data_process(root_path = datapath)
    table_name = 'dnb_city_list%s' % hdargs["apps"]
    dnb_city_file_lst = dataloader.load_dnb_city_lst(db=dnbdbname, table=table_name)
    cityabbr = dnb_city_file_lst['cityabbr']
    citylongname = dnb_city_file_lst['citylongname']
    origin_comp_file = dnb_city_file_lst['origin_comp_file']
else:
    pass


clfile = [c + hdargs['apps'] for c in cityabbr]
ssfile = ['all_ww_' + c.replace(hdargs['apps'], '') + '_similarity' + hdargs['apps'] for c in clfile]
dlsub_ssfile = ['dlsub_' + c for c in ssfile]
rsfile = ['z_reason_' + c + '_similarity' + hdargs['otversion'] for c in cityabbr]

cfile = origin_comp_file
lfile = hdargs['ls_card']
cityname = citylongname

if hdargs["use_additional_feat"]:
    feat_ext = hdargs['apps'].replace('.csv','_add.csv')
    comp_feat_file = 'company_feat' + feat_ext
    loc_feat_file = 'location_feat' + feat_ext
else:
    comp_feat_file = 'company_feat' + hdargs['apps']
    loc_feat_file = 'location_feat' + hdargs['apps']

cid = hdargs["cid"]
bid = hdargs["bid"]