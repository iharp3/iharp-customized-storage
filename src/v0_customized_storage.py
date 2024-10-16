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
                        TEMPORAL_DELETES_CSV_FILE_PATH,
                        SPATIAL_DELETES_CSV_FILE_PATH,
                        RAW_DATA_PATH,
                        AGG_DATA_PATH
                        )

from utils.func import (
                        download_data_from_csv,
                        files_to_delete,
                        delete_files,
                       )


if __name__ == "__main__":
    
    ''' Determine data that can be pruned from user input '''
    files_to_delete(V_ZERO_USER_INPUT_FILE_PATH, TEMPORAL_DELETES_CSV_FILE_PATH, output_folder=RAW_DATA_PATH) # TODO: could add before download_data_from_csv in if statement
    files_to_delete(V_ZERO_USER_INPUT_FILE_PATH, SPATIAL_DELETES_CSV_FILE_PATH, output_folder=RAW_DATA_PATH, resolution='spatial') # TODO: same as above 

#     ''' Download data with API calls'''
# #     download_data_from_csv(V_ZERO_USER_INPUT_FILE_PATH)

#     ''' Process data'''
#     # Aggregate files temporally


#     # Delete files in temporal_files_to_delete
#     delete_files(TEMPORAL_DELETES_CSV_FILE_PATH, AGG_DATA_PATH)

#     # Aggregate remaining files spatially and RENAME to include spatial files

#     # Delete files in spatial_files_to_delete
#     delete_files(SPATIAL_DELETES_CSV_FILE_PATH, AGG_DATA_PATH)

#     ''' Store data and get metadata '''