import os
from datetime import datetime, timedelta
import pandas as pd
import xarray as xr
import subprocess

from utils import load_csv, save_csv, save_list_to_csv, get_raw_file_name, get_data_path, delete_files, send_files_to_513
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

def download_data(row):
    try:
        raw_file_name = get_raw_file_name(row['variable'])
        completed_row = process_row(row, raw_file_name)
        print(f"\tDownloaded row: \n\t{completed_row}")
        save_csv([completed_row], get_data_path(config.EXTRA))
        return completed_row
    except Exception as e:
        print(f"\tError processing row {row}: \n\t{e}")

def aggregate_data(row):
    # Aggregate data and get metadata
    too_fine_list = []

    row_info = {'variable':row['variable'],'max_lat_N':row['max_lat_N'],'min_lat_S':row['min_lat_S'],'max_long_E':row['max_long_E'],'min_long_W':row['min_long_W'],'start_time':row['start_time'],'end_time':row['end_time']}
    metadata_list = []

    # Temporal aggregation
    print(f"\tStarting aggregation.")
    temporal_agg_object = DataAgg(name=row['file_name'], var=row['variable'], t=True, target=row['temporal_resolution'], constant=config.RAW_SP_RES)
    temporal_metadata_list, too_fine = temporal_agg_object.make_temporal_agg_files()
    too_fine_list += too_fine
    
    for d in temporal_metadata_list:
        print(f"\tSpatial agg")
        # Spatial aggregation
        spatial_agg_object = DataAgg(name=d['file_name'], var=row['variable'], t=False, target=row['spatial_resolution'], constant=d['temporal_resolution'])
        spatial_metadata_list, too_fine  = spatial_agg_object.make_spatial_agg_files(cur_t_agg_type=d['temporal_agg_type'])
        too_fine_list += too_fine
        metadata_list = metadata_list + spatial_metadata_list
        
    metadata_list = [{**row_info, **m} for m in metadata_list]
    save_csv(metadata_list, get_data_path(config.METADATA))
    save_csv(too_fine_list, get_data_path(config.EXTRA))

    return metadata_list

if __name__ == "__main__":

    new_home = "/export/scratch/uribe055"
    os.environ["HOME"] = new_home

    user_interest = load_csv(get_data_path(config.UI))
    full_metadata_list = []

    for row in user_interest:
        er = 0
        try:
        # download one row
            downloaded_row = download_data(row) # also saved to config.EXTRA
            er = 1

        # aggregate
            metadata_list, too_fine_list = aggregate_data(downloaded_row)  # also saved to config.METADATA, config.EXTRA
            er = 2

        # send files in metadata to 513
            send_files_to_513(csv_file=config.METADATA, remote_folder=config.VARIABLE)
            er = 3

        # delete all other files
            delete_files(filenames=too_fine_list, directory=config.CUR_DATA_D)
            er = 4
        except Exception as e:
            if er == 0:
                print(f"Error downloading row {row}:\n{e}")
            elif er == 1:
                print(f"Error aggregating row {row}:\n{e}")
            elif er == 2:
                print(f"Error sending files for row {row} to 513:\n{e}")
            elif er == 3:
                print(f"Error deleting files for row {row}: \n{e}")
            else:
                print(f"ERROR ERROR ERROR: \n{e}")