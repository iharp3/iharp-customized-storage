'''
customized_storage.py: Main function that creates customized storage.
'''
import os
import time
import sys

from utils import load_csv, save_csv, save_list_to_csv, get_raw_file_name, wait_for_file
from DataAgg import DataAgg
from ApiGenerator import API_Call

import config

def main():
    # Upload user input
    user_interest_rows = load_csv(config.USER_INTEREST)
    
    failed_rows = []

    for row in user_interest_rows:

        raw_file_name = get_raw_file_name(row['variable'])
        row['file_name'] = raw_file_name

        call = API_Call(row, raw_file_name)
        call.era5_api_request()
        call.make_file()

        if wait_for_file(raw_file_name):
            print(f"\tData downloaded to: {raw_file_name}.")
        else:
            failed_rows.append(row)

    # Note failed rows
    if failed_rows == []:
        print(f"Finished downloading all data.")
    else:
        if len(failed_rows) == len(user_interest_rows):
            print(f"Failed to download all files. Exiting.")
            sys.exit()
        else:
            print(f"Failed to download following files:")
            for row in failed_rows:
                print(f"\tVariable {row['variable']} for region {row['max_lat_N']}N {row['min_lat_S']}S {row['max_long_E']}E {row['min_long_W']}W at a \n\t\t{row['spatial_resolution']} spatial resolution and \n\ttime range {row['start_time']}-{row['end_time']} at a \n\t\t{row['temporal_resolution']}.")
    
    # Aggregate data and get metadata
    full_metadata_list = []

    print(f"Starting aggregation.")
    for row in user_interest_rows:
        row_info = {'variable':row['variable'],'max_lat_N':row['max_lat_N'],'min_lat_S':row['min_lat_S'],'max_long_E':row['max_long_E'],'min_long_W':row['min_long_W'],'start_time':row['start_time'],'end_time':row['end_time']}
        metadata_list = []

        # Temporal aggregation
        temporal_agg_object = DataAgg(name=row['file_name'], var=row['variable'], t=True, target=row['temporal_resolution'], constant=config.RAW_SP_RES)
        temporal_metadata_list = temporal_agg_object.make_temporal_agg_files()
        # [{'name':123, 'size':4, 'temp_res': d, 'sp_res':0.25}, {'name':456, 'size':4, 'temp_res': d, 'sp_res':0.25}]

        for d in temporal_metadata_list:
            # Spatial aggregation
            spatial_agg_object = DataAgg(name=d['file_name'], var=row['variable'], t=False, target=row['spatial_resolution'], constant=d['temporal_resolution'])
            spatial_metadata_list  = spatial_agg_object.make_spatial_agg_files()

            metadata_list = metadata_list + spatial_metadata_list
        
        metadata_list = [{**row_info, **m} for m in metadata_list]
        full_metadata_list = full_metadata_list + metadata_list
        print(f"\tCurrent metadata: \n\t{metadata_list}")

        # TODO: add an interim save that saves metadata list here so you don't lose all metadata data if loop fails

    print(f"All files temporally and spatially aggregated.")

    # Save metadata
    save_csv(full_metadata_list, config.METADATA)
    print(f"All metadata saved to {config.METADATA}")
        
if __name__ == "__main__":
    main()