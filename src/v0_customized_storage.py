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
                        RAW_P,
                        AGG_P,
                        U_IN_F,
                        T_DEL_F,
                        S_DEL_F
                        )

from utils.func import (
                        download_data_from_csv,
                        files_to_delete,
                        delete_files,
                        temporal_aggregation,
                        spatial_aggregation,
                        get_list_of_files_in_folder
                       )


if __name__ == "__main__":
    
    ''' Determine data that can be pruned from user input ''' 
    files_to_delete(U_IN_F, T_DEL_F, output_folder=RAW_P)
    files_to_delete(U_IN_F, S_DEL_F, output_folder=AGG_P, resolution='spatial') 

    ''' Download data with API calls'''
    # download_data_from_csv(U_IN_F)

    ''' Process data'''
    # Aggregate all raw files temporally
    temporal_aggregation(input_csv=U_IN_F, input_folder_path=RAW_P, output_folder_path=RAW_P)    # input and output folder paths both RAW_P so we spatially aggregate hourly files too

    # Delete files in temporal_files_to_delete
    delete_files(T_DEL_F)

    # Aggregate remaining files spatially
    all_metadata = spatial_aggregation(input_folder_path=RAW_P, output_folder_path=AGG_P)   # output_folder_path should match output_folder in files_to_delete function

    # Delete files in spatial_files_to_delete
    delete_files(S_DEL_F)

    ''' Store data and get metadata '''
    # TODO: get list of all files after deletion. these are ones you want to keep
    #       the metadata file is all of them. 
    #       add variable, location and time range to the metadata table...based on id_number??
    to_keep = get_list_of_files_in_folder(AGG_P)

    final_metadata = [lst for lst in all_metadata if lst[-1] in to_keep]
    
