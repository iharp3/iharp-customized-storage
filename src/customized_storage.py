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

from utils.func import (
                        get_time_range_ids,
                        get_location_range_ids
                        )


'''Upload user input csv'''
user_input_df = pd.read_csv(USER_INPUT_FILE_PATH)

# turn start and end time into datetime objects
user_input_df[col_name['start_t']] = pd.to_datetime(user_input_df[col_name['start_t']], format='%d-%m-%Y')
user_input_df[col_name['end_t']] = pd.to_datetime(user_input_df[col_name['end_t']], format='%d-%m-%Y')

'''Get necessary API calls and keep track of resolutions needed'''
# TODO: use groupby and apply for this

api_calls = []
to_delete = []

unique_vars = user_input_df[col_name['var']].unique()

for var in unique_vars:

    # get all rows with this variable
    var_df = user_input_df[user_input_df[col_name['var']] == var]
    
    # get temporal overlap info
    var_time_ranges_df = get_time_range_ids(var_df)

    # get spatial overlap info
    var_overlaps = get_location_range_ids()


'''Download data'''

'''Process data'''

'''Store data and get metadata'''
