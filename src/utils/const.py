'''
const.py

DESCRIPTION: Contains constant values that are used in customized_storage.py

Author: Ana Uribe
'''

# RAW_DATA_PATH = "/data/customized/raw/"
# AGG_DATA_PATH = "/data/customized/agg/"

# USER_INPUT_FILE_PATH = "/data/customized/user_input.csv"

RAW_DATA_PATH = "/home/uribe055/iharp-custumized-storage/data/raw/"
AGG_DATA_PATH = "/home/uribe055/iharp-custumized-storage/data/agg/"

USER_INPUT_FILE_PATH = "//home/uribe055/iharp-custumized-storage/data/user_input.csv"

long_short_name_dict = {
    "2m_temperature": "t2m",
}

col_name = {
    "var": "variable",
    "t_res": "temporal_resolution",
    "s_res":"spatial_resolution",
    "max_lat": "max_latitude",
    "min_lat": "min_latitude",
    "max_long": "max_longitude",
    "min_long": "min_longitude",
    "start_t": "start_time",
    "end_t": "end_time"
}