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
                        raw_p,
                        agg_p,
                        user_input_f,
                        t_deletes_f,
                        s_deletes_f
                        )

from utils.func import (
                        download_data_from_csv,
                        files_to_delete,
                        delete_files,
                       )


if __name__ == "__main__":
    
    ''' Determine data that can be pruned from user input '''
    
    # files_to_delete(V_ZERO_USER_INPUT_FILE_PATH, TEMPORAL_DELETES_CSV_FILE_PATH, output_folder=RAW_DATA_PATH)
    # files_to_delete(V_ZERO_USER_INPUT_FILE_PATH, SPATIAL_DELETES_CSV_FILE_PATH, output_folder=RAW_DATA_PATH, resolution='spatial') 
    files_to_delete(user_input_f, t_deletes_f, output_folder=raw_p)
    files_to_delete(user_input_f, s_deletes_f, output_folder=raw_p, resolution='spatial') 

    ''' Download data with API calls'''
    # download_data_from_csv(V_ZERO_USER_INPUT_FILE_PATH)

    ''' Process data'''
    # Aggregate all raw files temporally


    # Delete files in temporal_files_to_delete
    # temp_files = delete_files(TEMPORAL_DELETES_CSV_FILE_PATH)

    # Aggregate remaining files (temp_files) spatially

    # Delete files in spatial_files_to_delete
    # spatio_temp_files = delete_files(SPATIAL_DELETES_CSV_FILE_PATH)

    ''' Store data and get metadata '''