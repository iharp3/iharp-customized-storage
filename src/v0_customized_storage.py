'''
v0_customized_storage.py

DESCRIPTION: Takes in user input and downloads the desired ERA5 data from CDS.
             
INPUT: (csv) user input with [variable,max_lat,min_lat,max_long,min_long,start_year,start_month,start_day,start_time,end_year,end_month,end_day,end_time,file_path]

OUTPUT: data stored in local storage (Data Management Lab cluster: computer __)
        metadata for local storage

Author: Ana Uribe
'''

'''Imports'''

import csv

from utils.const import (
                        RAW_P,
                        AGG_P,
                        U_IN_F,
                        T_DEL_F,
                        S_DEL_F,
                        M_F
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
    print("\n\nStarting download of data.")
    # download_data_from_csv(U_IN_F)

    ''' Process data'''
    # Aggregate all raw files temporally
    print("\n\nData downloaded successfully, starting temporal aggregation.")
    temporal_aggregation(input_csv=U_IN_F, input_folder_path=RAW_P, output_folder_path=RAW_P)    # input and output folder paths both RAW_P so we spatially aggregate hourly files too
    print("\n\nTemporal aggregation complete, starting pruning.")

    # Delete files in temporal_files_to_delete
    delete_files(T_DEL_F)

    # Aggregate remaining files spatially
    print("\n\nTemporal pruning complete, starting spatial aggregation.")
    all_metadata = spatial_aggregation(input_folder_path=RAW_P, output_folder_path=AGG_P)   # output_folder_path should match output_folder in files_to_delete function
    print("\n\nSpatial aggregation complete, starting pruning.")
    
    # Delete files in spatial_files_to_delete
    delete_files(S_DEL_F)

    ''' Store data and get metadata '''
    to_keep = get_list_of_files_in_folder(AGG_P)

    filtered_metadata = [lst for lst in all_metadata if lst[-1] in to_keep]

    # TODO:  add variable, location and time range from user input to the metadata table...based on id_number??
    final_metadata = [[]]

    # Create a CSV file and write the data
    with open(M_F, mode='w', newline='') as file:
        writer = csv.writer(file)
        
        # Write rows of data
        writer.writerows(final_metadata)

    print(f"\n\nCustomized storage hase been built.")
    print(f"\n\tStorage based on user input in: {U_IN_F}.")
    print(f"\n\tMetadata stored in {M_F}.")

