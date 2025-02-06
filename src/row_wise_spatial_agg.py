import os
from datetime import datetime, timedelta
import pandas as pd
import xarray as xr
import subprocess

from utils import load_csv, save_csv, get_raw_file_name, get_data_path, send_files_to_514
from DataAggBaseStats import DataAgg
from ApiGenerator import API_Call

import config

def aggregate_data(row):
    # Aggregate data and get metadata

    row_info = {'variable':row['variable'],'max_lat_N':row['max_lat_N'],'min_lat_S':row['min_lat_S'],'max_long_E':row['max_long_E'],'min_long_W':row['min_long_W'],'start_time':row['start_time'],'end_time':row['end_time']}

    spatial_agg_object = DataAgg(name=row['file_name'], var=row['variable'], t=False, target=row['spatial_resolution'], constant=row['temporal_resolution'])
    spatial_metadata_list  = spatial_agg_object.make_spatial_agg_files(cur_t_agg_type=row['temporal_agg_type'])

    metadata_list = [{**row_info, **m} for m in spatial_metadata_list]
    print(metadata_list)
    return metadata_list

if __name__ == "__main__":

    temp_agg_metadata = load_csv('/data/iharp-customized-storage/storage/514_agg/2m_temperature/temporal_agg_metadata.csv')
    full_metadata_list = [] 
    try:
        for row in temp_agg_metadata:
            metadata_list = aggregate_data(row)  # also saved to config.METADATA, config.TO_DELETE
            # send_files_to_514(csv_file=metadata_list)
            full_metadata_list += metadata_list
            save_csv(full_metadata_list, '/data/iharp-customized-storage/storage/514_agg/2m_temperature/p_metadata.csv')

        save_csv(full_metadata_list, '/data/iharp-customized-storage/storage/514_agg/2m_temperature/metadata.csv')
    except Exception as e:
        print(f"\n\n\nSomething happened...\n\trow: {row}\n\n\trow type: {type(row)} \n\n\tError:\n{e}")