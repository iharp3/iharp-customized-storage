'''
customized_storage.py: Main function that creates customized storage.
'''
import os
import time
import sys
import pandas as pd
import xarray as xr

from utils import load_csv, save_csv, save_list_to_csv, get_raw_file_name, get_unique_num
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
                completed_rows.append(process_row(row, raw_file_path))
                
                print(f"\tData downloaded to: {raw_file_name}.")
                print(completed_rows)
                save_csv(completed_rows, ui_named)  # saves current user interest rows with name to a csv
                print('save_csv worked.')
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

def combine_data(all_named, folder):
    '''TODO: Assumes the variable column is the same for all rows'''
    df = pd.concat([pd.read_csv(file) for file in all_named], ignore_index=True)
    group_cols = ['start_time', 'end_time', 'temporal_resolution', 'spatial_resolution', 'max_lat_N', 'min_lat_S']
    grouped = df.groupby(group_cols)

    final_ui_named = [] # csv with condensed user interest files
    list_of_combined_files = []
    u = get_unique_num()
    for _, group in grouped:
        sorted_group = group.sort_values(by='max_long_E')
        consecutive_files = []
        final_min_long_W = 0
        
        i = 0
        for i in range(len(sorted_group)):
            if i == 0:
                consecutive_files.add(sorted_group.iloc[i]['file_name'])
            else:
                prev_row = sorted_group.iloc[i-1]
                cur_row = sorted_group.iloc[i]
                if prev_row['min_long_W'] == cur_row['max_long_E']:
                    consecutive_files.add(cur_row['file_name'])
                    final_min_long_W = cur_row['min_long_W']
                else:
                    # add the remaining files to final_ui_named
                    for j in range(len(sorted_group) - i):
                        final_ui_named.append(sorted_group.iloc[j + i])
                    break
        
        if len(consecutive_files) == 1:     # no consecutive files, add first row to final_ui_named
            final_ui_named.append(sorted_group.iloc[0])
        else:
            # concatenate consecutive files
            file_paths = [os.path.join(config.CUR_DATA_D, file) for file in consecutive_files]
            datasets = [xr.open_dataset(file) for file in file_paths]
            merged_dataset = xr.concat(datasets, dim='longitude')
            for ds in datasets:
                ds.close()
            # save concatenation to new file
            merged_dataset_name = get_raw_file_name(sorted_group.iloc[0]['variable'])
            merged_dataset.to_netcdf(merged_dataset_name)
            # add row to final_ui_named
            merged_row = sorted_group.iloc[0]
            merged_row['min_long_W'] = final_min_long_W
            merged_row['file_name'] = merged_dataset_name
            final_ui_named.append(merged_row)

        list_of_combined_files += consecutive_files

        # update final_ui_named csv with each group
        csv_name = sorted_group.iloc[0]['variable'] + f'_final_user_interest_{u}.csv'
        csv_path = os.path.join(folder, csv_name)
        save_csv(final_ui_named, csv_path)
    
    # save list of files that were combined and can be deleted
    combined_files_name = sorted_group.iloc[0]['variable'] + f'delete_combined_files_{u}.csv'
    combined_files_path = os.path.join(folder, combined_files_name)
    save_csv(list_of_combined_files, combined_files_path)

    return csv_path, combined_files_path

def aggregate_data(ui_named, folder):
    # Aggregate data and get metadata
    full_metadata_list = []
    user_interest_named = load_csv(ui_named)

    too_fine_list = []

    print(f"Starting aggregation.")
    for row in user_interest_named:
        print(f"\nNEW ROW\n")
        row_info = {'variable':row['variable'],'max_lat_N':row['max_lat_N'],'min_lat_S':row['min_lat_S'],'max_long_E':row['max_long_E'],'min_long_W':row['min_long_W'],'start_time':row['start_time'],'end_time':row['end_time']}
        metadata_list = []

        # Temporal aggregation
        temporal_agg_object = DataAgg(name=row['file_name'], var=row['variable'], t=True, target=row['temporal_resolution'], constant=config.RAW_SP_RES)
        temporal_metadata_list, too_fine = temporal_agg_object.make_temporal_agg_files()
        # [{'name':123, 'size':4, 'temp_res': d, 'sp_res':0.25}, {'name':456, 'size':4, 'temp_res': d, 'sp_res':0.25}]
        too_fine_list += too_fine
        for d in temporal_metadata_list:
            print(f"\tSpatial agg")
            # Spatial aggregation
            spatial_agg_object = DataAgg(name=d['file_name'], var=row['variable'], t=False, target=row['spatial_resolution'], constant=d['temporal_resolution'])
            spatial_metadata_list, too_fine  = spatial_agg_object.make_spatial_agg_files(cur_t_agg_type=d['temporal_agg_type'])
            too_fine_list = too_fine
            metadata_list = metadata_list + spatial_metadata_list
        
        metadata_list = [{**row_info, **m} for m in metadata_list]
        full_metadata_list = full_metadata_list + metadata_list
        save_csv(full_metadata_list, config.METADATA)

    print(f"All files temporally and spatially aggregated.")
    # Save metadata
    save_csv(full_metadata_list, config.METADATA)
    print(f"All metadata saved to {config.METADATA}")

    # Save files to delete
    u = get_unique_num()
    too_fine_path = os.path.join(folder, f'delete_{u}.csv')
    save_csv(too_fine_list, too_fine_path)

    return too_fine_list

if __name__ == "__main__":
    all_named = []
    for i in config.UI_LIST:
        cur_ui = os.path.join(config.CUR_DATA_D, i)
        named = 'named_' + cur_ui
        all_named.append(named)
        failed = 'failed' + cur_ui

        download_data(cur_ui, named, failed)

    # final_info_folder = os.path.join(config.CUR_DATA_D, 'merged')

    # final_named, combined_files = combine_data(all_named, final_info_folder)

    # files_to_delete = aggregate_data(final_named, final_info_folder)

    # if config.DELETE:
        # delete_files(filenames=combined_files, directory=config.CUR_DATA_D)   # deletes files that were combined
        # delete_files(filenames=files_to_delete, directory=config.CUR_DATA_D)  # deletes files that are too fine-grained

        # print('All files in {combined_files} and {files_to_delete} have been deleted.')