'''
customized_storage.py: Main function that creates customized storage.
'''
import os
import time
import sys

import config
from utils import load_csv
from ApiGenerator import API_Call

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
    if failed_rows == []
        print(f"Finished downloading all data.")
    else:
        if len(failed_rows) == len(user_interest_rows):
            print(f"Failed to download all files. Exiting.")
            sys.exit()
        else
            print(f"Failed to download following files:")
            for row in failed_rows:
                print(f"\tVariable {row['variable']} for region {row['max_lat_N']}N {row['min_lat_S']}S {row['max_long_E']}E {row['min_long_W']}W at a \n\t\t{row['spatial_resolution']} spatial resolution and \n\ttime range {row['start_time']}-{row['end_time']} at a \n\t\t{row['temporal_resolution']}.")

    # Aggregate files
    full_metadata_list = []
    full_to_delete_list = []

    for row in user_interest_rows:
        row_info = {'variable':row['variable'],'max_lat_N':row['max_lat_N'],'min_lat_S':row['min_lat_S'],'max_long_E':row['max_long_E'],'min_long_W':row['min_long_W'],'start_time':row['start_time'],'end_time':row['end_time']}

        # Temporal aggregation
        temporal_agg_object = DataAgg(name=row['file_name'], t=True, target=row['temporal_resolution'])
        temporal_metadata_list, temporal_to_delete_list = temporal_agg_object.make_temporal_agg_files()
        # [{'name':123, 'size':4, 'temp_res': d, 'sp_res':0.25}, {'name':456, 'size':4, 'temp_res': d, 'sp_res':0.25}]

        for d in metadata_list:
            # Spatial aggregation
            spatial_agg_object, to_delete_list = DataAgg(name=d['file_name'], t=False, target=row['spatial_resolution'])
            spatial_metadata_list, temporal_to_delete_list = spatial_agg_object.make_spatial_agg_files()

            temporal_metadata_list = temporal_metadata_list + spatial_metadata_list
        
        metadata_list = [{**row_info, **m} for m in metadata_list]
        full_metadata_list = full_metadata_list + metadata_list
        full_to_delete_list = full_to_delete_list + to_delete_list


        # Temporally aggregate file (return metadata with list of file names to aggregate spatially, and list of files to delete)
            # Spatially aggregate file (return metadata and list of files to delete)

    # Save metadata
    # Check before deleting files to delete


    # turn start and end time into datetime objects
    # user_input_df[col_name['start_t']] = pd.to_datetime(user_input_df[col_name['start_t']], format='%d-%m-%Y')
    # user_input_df[col_name['end_t']] = pd.to_datetime(user_input_df[col_name['end_t']], format='%d-%m-%Y')

    '''Get necessary API calls and keep track of resolutions needed'''
    # TODO: use groupby and apply for this

if __name__ == "__main__":
    main()