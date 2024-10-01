'''
customized_storage.py

DESCRIPTION: Takes in user input and creates customized storage.
             
INPUT: (csv) user input with [variable, max_lat, min_lat, max_long, min_long, start_year, end_year, resolution]

OUTPUT: data stored in local storage (Data Management Lab cluster: computer __)
        metadata for local storage

Author: Ana Uribe
'''

'''Imports'''
import pandas as pd

from utils.const import (
                        USER_INPUT_FILE_PATH, 
                        RAW_DATA_PATH, 
                        AGG_DATA_PATH,
                        csv_col_names_dict,
                        )


'''Upload user input csv'''
user_input_df = pd.read_csv(USER_INPUT_FILE_PATH)

'''Get necessary API calls [and data processing steps?]'''
unique_vars = user_input_df[csv_col_names_dict["var"]].unique()

for var in unique_vars:
    # get all rows with this variable
    var_df = user_input_df[user_input_df[csv_col_names_dict["var"]] == var]
    
    # get rows that need to be downloaded
#     data_slices_df = get_data_slices(var_df)

    # check if any lower resolutions are located (in time and space) within a higher resolution
    res_hierarchy = {
        'hour': ['day', 'month', 'year'],
        'day': ['month', 'year'],
        'month': ['year'],
        'year': [] 
        }
    
    for index, row in var_df.iterrows():
        current_res = row[csv_col_names_dict["res"]]
        lower_resolutions = res_hierarchy.get(current_res, [])

        if lower_resolutions:
                filtered_rows = var_df[var_df['resolution'].isin(lower_resolutions)]
                
    pass

    



'''Download data'''

'''Process data'''

'''Store data and get metadata'''
