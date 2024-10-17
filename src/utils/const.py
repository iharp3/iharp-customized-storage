'''
const.py

DESCRIPTION: Contains constant values that are used in customized_storage.py

Author: Ana Uribe
'''
import os

home_dir = os.path.expanduser('~')
repo = 'iharp-customized-storage'

RAW_P = os.path.join(home_dir, repo, 'data/raw')
AGG_P = os.path.join(home_dir, repo, 'data/agg')

U_IN_F = os.path.join(home_dir, repo, 'data/user_input.csv')

T_DEL_F = os.path.join(home_dir, repo, 'data/prune_list_temporal.csv')
S_DEL_F = os.path.join(home_dir, repo, 'data/prune_list_spatial.csv')
M_F = os.path.join(home_dir, repo, 'data/agg/metadata.csv')

api_request_settings = {
    "dataset": "reanalysis-era5-single-levels",
    "product_type": ["reanalysis"],
    "data_format": "netcdf",
    "download_format": "unarchived",
    "month": [str(i).zfill(2) for i in range(1, 13)],
    "day": [str(i).zfill(2) for i in range(1, 32)],
    "time":[f"{str(i).zfill(2)}:00" for i in range(0, 24)]
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