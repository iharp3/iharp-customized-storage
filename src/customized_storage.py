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

from utils.const import (
                        USER_INPUT_FILE_PATH, 
                        RAW_DATA_PATH, 
                        AGG_DATA_PATH,
                        col_names_dict,
                        )


'''Upload user input csv'''
user_input_df = pd.read_csv(USER_INPUT_FILE_PATH)

'''Get necessary API calls and keep track of resolutions needed'''

unique_vars = user_input_df[col_names_dict["var"]].unique()

for var in unique_vars:

    # get all rows with this variable
    var_df = user_input_df[user_input_df[col_names_dict["var"]] == var]
    
    # 

    



'''Download data'''

'''Process data'''

'''Store data and get metadata'''
