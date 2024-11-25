'''
const.py

DESCRIPTION: Contains constant values that are used in customized_storage.py

Author: Ana Uribe
'''
import os

# cluster_501 = False

# if not cluster_501:
#     home_dir = os.path.expanduser('~')
# else:
#     home_dir = '/export/scratch'

home_dir = os.path.expanduser('~')
repo = 'iharp-customized-storage'

DATA_P = os.path.join(home_dir, repo, 'data')
u_in_file_name = 'user_input.csv'
U_IN_F = os.path.join(home_dir, repo, f'data/files/{u_in_file_name}')
M_F_UNORDERED = os.path.join(home_dir, repo, 'data/files/unordered_metadata.csv')
M_F = os.path.join(home_dir, repo, 'data/files/metadata.csv')


if __name__ == "__main__":
    print('\n##############################################\n')
    print('\n################### Paths ###################\n')
    print(f'\nData will be saved in: {DATA_P}')
    print(f'\nUser interest input will be taken from: {U_IN_F}')
    print(f'\nMetadata will be saved in: {M_F}')
    print('\n##############################################\n')
    print('\n##############################################\n')

RAW_RESOLUTION = 0.25 # 0.25 x 0.25 degree

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