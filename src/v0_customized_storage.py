'''
v0_customized_storage.py

DESCRIPTION: Takes in user input and downloads the desired ERA5 data from CDS.
             
INPUT: (csv) user input with [variable,max_lat,min_lat,max_long,min_long,start_year,start_month,start_day,start_time,end_year,end_month,end_day,end_time,file_path]

OUTPUT: data stored in local storage (Data Management Lab cluster: computer __)
        metadata for local storage

Author: Ana Uribe
'''

'''Imports'''
import pandas as pd
from datetime import datetime

from utils.const import (
                        V_ZERO_USER_INPUT_FILE_PATH, 
                        RAW_DATA_PATH, 
                        AGG_DATA_PATH,
                        col_name,
                        )

from utils.func import (
                        download_data_from_csv
                       )

'''Upload user input csv'''
user_input_df = pd.read_csv(V_ZERO_USER_INPUT_FILE_PATH)

''' Download data with API calls'''
if __name__ == "__main__":
    
    download_data_from_csv(V_ZERO_USER_INPUT_FILE_PATH)

''' Process data'''


''' Store data and get metadata '''