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
from dask.distributed import LocalCluster
import pandas as pd

from utils.const import (
                        DATA_P,
                        U_IN_F,
                        M_F_UNORDERED,
                        M_F,
                        col_name
                        )

from utils.func import (
                        download_data_from_csv,
                        files_to_delete,
                        delete_files,
                        temporal_aggregation,
                        spatial_aggregation,
                        filter_metadata
                       )


if __name__ == "__main__":
    
    ''' Determine data that can be pruned from user input ''' 
    t_files_to_delete, s_files_to_delete = files_to_delete(U_IN_F, output_folder=DATA_P)


    ''' Download data with API calls'''
    print("\n\nStarting download of data.")
    # download_data_from_csv(U_IN_F)

    print("\n\nData downloaded successfully, starting temporal aggregation.")

    ''' Initiate Cluster '''
    cluster = LocalCluster(n_workers=10)  # Fully-featured local Dask cluster
    client = cluster.get_client()

    temporal_aggregation(input_csv=U_IN_F, input_folder_path=DATA_P, output_folder_path=DATA_P, c=client)    # input and output folder paths both RAW_P so we spatially aggregate hourly files too
    cluster.close()

    print("\n\nTemporal aggregation complete, starting pruning.")

    '''Delete files in temporal_files_to_delete'''
    delete_files(t_files_to_delete)

    # Aggregate remaining files spatially
    print("\n\nTemporal pruning complete, starting spatial aggregation.")

    ''' Initiate Cluster '''
    cluster = LocalCluster(n_workers=10) 
    client = cluster.get_client()

    all_metadata = spatial_aggregation(user_input_csv=U_IN_F, input_folder_path=DATA_P, output_folder_path=DATA_P, c=client)   # output_folder_path should match output_folder in files_to_delete function
    cluster.close()

    print("\n\nSpatial aggregation complete, starting pruning.")
    
    '''Delete files in spatial_files_to_delete'''
    delete_files(s_files_to_delete)

    ''' Store data and get metadata '''
    filtered_metadata = filter_metadata(t_files_to_delete, s_files_to_delete, all_metadata)

    # Create a CSV file and write the data
    with open(M_F_UNORDERED, mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerows(filtered_metadata)

    # sort by id
    df = pd.read_csv(M_F_UNORDERED)
    sorted_df = df.sort_values(by=col_name['path'])

    # Save the sorted DataFrame to a new CSV file
    sorted_df.to_csv(M_F, index=False)

    print(f"\n\nCustomized storage hase been built.")
    print(f"\n\tStorage based on user input in: {U_IN_F}.")
    print(f"\n\tMetadata stored in {M_F}.")

