import os

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
compstak_dnb_match_file = 'compstak_dnb_v2.csv'


kwargs_key = 'task_instance'

#in the future it can be moved into Variables
hdargs = {
    "run_root":"/home/ubuntu/location_recommender_system",
    "ls_card":"location_scorecard_200106.csv",
    "apps":"_200106.csv",
    "otversion":"_200106.csv",
    "reason_col_name":{
        "reason_similar_biz":
            {"p":1,"useFLG":1},
        "reason_location_based":
            {"p":7,"useFLG":1},
        "reason_model_based":
            {"p":8,"useFLG":1},
        "reason_similar_location":
            {"p":6,"useFLG":0},
        "reason_similar_company":
            {"p":5,"useFLG":0},
        "reason_close_2_current_location":
            {"p":2,"useFLG":1},
        "reason_inventory_bom":
            {"p": 3, "useFLG": 1},
        "reason_compstak":
            {"p": 4, "useFLG": 1}
    },
    "cid":"duns_number",
    "bid":"atlas_location_uuid",
    "test_db":"tmp_table",
    "dev_db":"reason_table"
}


datapath = hdargs['run_root']
datapath_mid = pj(datapath, hdargs["test_db"])

clfile = [c + hdargs['apps'] for c in cityabbr]
ssfile = ['all_ww_' + c.replace(hdargs['apps'], '') + '_similarity' + hdargs['apps'] for c in clfile]
dlsub_ssfile = ['dlsub_' + c for c in ssfile]
rsfile = ['z_reason_' + c + '_similarity' + hdargs['otversion'] for c in cityabbr]

cfile = origin_comp_file
lfile = hdargs['ls_card']
cityname = citylongname
comp_feat_file = 'company_feat' + hdargs['apps']
loc_feat_file = 'location_feat' + hdargs['apps']

cid = hdargs["cid"]
bid = hdargs["bid"]