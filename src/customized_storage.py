'''
customized_storage.py

DESCRIPTION: Takes in user input and creates customized storage.
             
INPUT: (csv) user input with [variable, max_lat, min_lat, max_long, min_long, start_year, end_year, ]

OUTPUT: data stored in local storage (Data Management Lab cluster: computer __)
        metadata for local storage

Author: Ana Uribe
'''

'''Imports'''
import pandas as pd
from datetime import datetime

from utils.const import (
                        USER_INPUT_FILE_PATH, 
                        RAW_DATA_PATH, 
                        AGG_DATA_PATH,
                        col_name,
                        )

from utils.func import (get_time_range_ids)


'''Upload user input csv'''
user_input_df = pd.read_csv(USER_INPUT_FILE_PATH)
unique_vars = user_input_df[col_name['var']].unique()

# turn start and end time into datetime objects
user_input_df[col_name['start_t']] = pd.to_datetime(user_input_df[col_name['start_t']], format='%d-%m-%Y')
user_input_df[col_name['end_t']] = pd.to_datetime(user_input_df[col_name['end_t']], format='%d-%m-%Y')

'''Get necessary API calls and keep track of resolutions needed'''
api_calls = []
to_delete = []

for var in unique_vars:

    # get all rows with this variable
    var_df = user_input_df[user_input_df[col_name['var']] == var]
    
    # check if times overlap
    var_time_ranges_df = get_time_range_ids(var_df)

    # check if locations overlap
    unique_time_ranges = var_time_ranges_df[col_name['t_r_id']].unique()
    for r in unique_time_ranges:
        pass

'''Download data'''

'''Process data'''

'''Store data and get metadata'''
