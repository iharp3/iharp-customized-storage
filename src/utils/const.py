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

V_ZERO_USER_INPUT_FILE_PATH = "//home/uribe055/iharp-custumized-storage/data/v0_user_input.csv"

api_request_settings = {
    "dataset": "reanalysis-era5-single-levels",
    "data_format": "netcdf"
}

long_short_name_dict = {
    "2m_temperature": "t2m",
}

col_name = {
    "var": "variable",
    "t_res": "temporal_resolution",
    "s_res":"spatial_resolution",
    "max_lat": "max_lat",
    "min_lat": "min_lat",
    "max_long": "max_long",
    "min_long": "min_long",
    "s_y": "start_year",
    "s_m": "start_month",
    "s_d": "start_day",
    "s_t": "start_time",
    "start_datetime": "start_datetime",
    "e_y": "end_year",
    "e_m": "end_month",
    "e_d": "end_day",
    "e_t": "end_time",
    "end_datetime": "end_datetime",
    "t_r_id": "time_range_id",
    "path": "file_path"
}