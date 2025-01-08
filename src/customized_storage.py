'''
customized_storage.py: Main function that creates customized storage.
'''
import os
import time
import sys
import pandas as pd
import xarray as xr

from utils import load_csv, save_csv, save_list_to_csv, get_raw_file_name, wait_for_file
from DataAgg import DataAgg
from ApiGenerator import API_Call

import config

def process_row(row, raw_file_name):
    updated_row = row
    updated_row['file_name'] = raw_file_name

    call = API_Call(row, raw_file_name)
    call.era5_api_request()
    call.make_file()

    return updated_row

def download_data(ui, ui_named, ui_failed):
    try:
        # Upload user input
        user_interest_rows = load_csv(ui)

        failed_rows = []
        completed_rows = []

        for row in user_interest_rows:
            try:
                raw_file_name = get_raw_file_name(row['variable'])
                raw_file_path = os.path.join(config.CUR_DATA_D, raw_file_name)
                completed_rows += process_row(row, raw_file_path)
                
                print(f"\tData downloaded to: {raw_file_name}.")
                save_csv(completed_rows, ui_named)  # saves current user interest rows with name to a csv

            except Exception as e:
                print(f"\tError processing row {row}: {e}")
                failed_rows.append(row)
        
        final_ui_named = 'final_' + ui_named
        save_csv(completed_rows, final_ui_named)
        # Note failed rows
        if failed_rows == []:
            print(f"Finished downloading all data.")
        else:
            # save failed rows to file
            print(f"Did not download all data. See {ui_failed}.")
            save_list_to_csv(failed_rows, ui_failed)

    except FileNotFoundError:
        print(f"Input file not found.")
    except Exception as e:
        print(f"An error occurred: {e}.")

def combine_data(all_named):

    df = pd.concat([pd.read_csv(file) for file in all_named], ignore_index=True)
    group_cols = ['start_time', 'end_time', 'temporal_resolution', 'spatial_resolution', 'max_lat_N', 'min_lat_S']
    grouped = df.groupby(group_cols)

    final_ui_named = []
    for _, group in grouped:
        sorted_group = group.sort_values(by='max_long_E')


def aggregate_data(ui_named):
    # Aggregate data and get metadata
    full_metadata_list = []
    user_interest_named = load_csv(ui_named)

    print(f"Starting aggregation.")
    for row in user_interest_named:
        print(f"\nNEW ROW\n")
        row_info = {'variable':row['variable'],'max_lat_N':row['max_lat_N'],'min_lat_S':row['min_lat_S'],'max_long_E':row['max_long_E'],'min_long_W':row['min_long_W'],'start_time':row['start_time'],'end_time':row['end_time']}
        metadata_list = []

        # Temporal aggregation
        temporal_agg_object = DataAgg(name=row['file_name'], var=row['variable'], t=True, target=row['temporal_resolution'], constant=config.RAW_SP_RES)
        temporal_metadata_list = temporal_agg_object.make_temporal_agg_files()
        # [{'name':123, 'size':4, 'temp_res': d, 'sp_res':0.25}, {'name':456, 'size':4, 'temp_res': d, 'sp_res':0.25}]

        for d in temporal_metadata_list:
            print(f"\tSpatial agg")
            # Spatial aggregation
            spatial_agg_object = DataAgg(name=d['file_name'], var=row['variable'], t=False, target=row['spatial_resolution'], constant=d['temporal_resolution'])
            spatial_metadata_list  = spatial_agg_object.make_spatial_agg_files(cur_t_agg_type=d['temporal_agg_type'])

            metadata_list = metadata_list + spatial_metadata_list
        
        metadata_list = [{**row_info, **m} for m in metadata_list]
        full_metadata_list = full_metadata_list + metadata_list
        save_csv(full_metadata_list, config.METADATA)

    print(f"All files temporally and spatially aggregated.")

    #TODO: raw files have to be deleted. Probably easiest to do in DataAgg before you add things to the metadata list...
    
    # Save metadata
    save_csv(full_metadata_list, config.METADATA)
    print(f"All metadata saved to {config.METADATA}")

if __name__ == "__main__":
    all_named = []
    for i in config.UI_LIST:
        cur_ui = os.path.join(config.CUR_DATA_D, i)
        named = cur_ui + 'named'
        all_named.append(named)
        failed = cur_ui + 'failed'

        download_data(cur_ui, named, failed)

    # combine_data(all_named)
    # aggregate_data(named)