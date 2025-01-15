'''
customized_storage.py: Main function that creates customized storage.
'''
import os
import time
from datetime import datetime, timedelta
import sys
import pandas as pd
import xarray as xr

from utils import load_csv, save_csv, save_list_to_csv, get_raw_file_name, get_unique_num, get_data_path, delete_files
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
                completed_rows.append(process_row(row, raw_file_name))
                
                print(f"\tData downloaded to: {raw_file_name}.")
                print(completed_rows)
                save_csv(completed_rows, get_data_path(ui_named))  # saves current user interest rows with name to a csv
            except Exception as e:
                print(f"\tError processing row {row}: {e}")
                failed_rows.append(row)
        
        final_ui_named = 'downloaded_named.csv'    # all csvs for each ui csv are saved to a final one
        save_csv(completed_rows, get_data_path(final_ui_named))
        # Note failed rows
        if failed_rows == []:
            print(f"Finished downloading all data.")
        else:
            # save failed rows to file
            print(f"Did not download all data. See {ui_failed}.")
            save_list_to_csv(failed_rows, get_data_path(ui_failed))

    except FileNotFoundError:
        print(f"Input file not found.")
    except Exception as e:
        print(f"An error occurred: {e}.")

def combine_data(all_named, all_named_together, grp_cols, sort_col, compared_col, merge_dim, t_bool):
    # load all file rows into one pandas df
    if os.path.isfile(get_data_path(all_named_together)):
        df = pd.read_csv(get_data_path(all_named_together))
    else:
        df = pd.concat([pd.read_csv(get_data_path(file)) for file in all_named], ignore_index=True)

    # group file rows by columns
    group_cols = grp_cols
    grouped = df.groupby(group_cols)

    final_ui_named = []
    list_of_combined_files = []
    u = get_unique_num()
    csv_name = f'combined_user_interest_{u}.csv'
    csv_path = get_data_path(csv_name)

    print(f"\nFound {len(grouped)} groups")
    for _, group in grouped:
        print(f"\tNew Group:")
        if len(group) == 1:
            d = group.iloc[0].to_dict()
            final_ui_named.append(d)
        else:
            # order max_long_E
            sorted_group = group.sort_values(by=sort_col, ascending=False)
            # check if consecutive
            consecutive_files = []
            count = 0
            for i in range(1, len(sorted_group)):
                prev_row = sorted_group.iloc[i-1].to_dict()
                cur_row = sorted_group.iloc[i].to_dict()
                # rows are consecutive
                if t_bool == True:
                    compared_date = datetime.strptime(cur_row[compared_col], "%Y-%m-%dT%H:%M")
                    sorted_date = datetime.strptime(prev_row[sort_col], "%Y-%m-%dT%H:%M")

                    if sorted_date - compared_date == timedelta(hours=1):
                        consecutive = True
                    else:
                        consecutive = False
                else:
                    consecutive = prev_row[compared_col] == cur_row[sort_col]
                if consecutive:
                    if (i-1) == 0:
                        consecutive_files.append(prev_row['file_name'])     # add first row to consecutive files
                    consecutive_files.append(cur_row['file_name'])
                    if t_bool:
                        final_min_long_W = cur_row[sort_col]
                    else:
                        final_min_long_W = cur_row[compared_col]
                    count = i
                # rows stop being consecutive
                else:
                    if (i-1) == 0:
                        count = 0
                    else:
                        count = i + 1
                    break
                    # TODO: make it so the second and third rows could be merged rather than just the first + others

            if len(consecutive_files) > 1:
                print(f"\t\t- {len(consecutive_files)} will be combined...")
                # combine consecutive files
                file_paths = [os.path.join(config.CUR_DATA_D, file) for file in consecutive_files]
                
                datasets = [xr.open_dataset(file) for file in file_paths]
                merged_dataset = xr.concat(datasets, dim=merge_dim)
                print(f"\t\t\t finished concatenation.")

                for ds in datasets:
                    ds.close()

                # # save concatenation to new file
                merged_dataset_name = get_raw_file_name(sorted_group.iloc[0]['variable'])
                merged_dataset.to_netcdf(get_data_path(merged_dataset_name))

                # add row to final_ui_named
                merged_row = sorted_group.iloc[0].to_dict()
                if t_bool:
                    merged_row[sort_col] = final_min_long_W
                else:
                    merged_row[compared_col] = final_min_long_W
                merged_row['file_name'] = merged_dataset_name
                final_ui_named.append(merged_row)

                list_of_combined_files += consecutive_files

            # if not all rows in sorted group were combined
            if count < (len(sorted_group)-1):
                # add the remaining files to final_ui_named
                for j in range(len(sorted_group) - count):
                    final_ui_named.append(sorted_group.iloc[j + count].to_dict())

            # update final_ui_named csv with each group
            save_csv(final_ui_named, csv_path)

    save_csv(final_ui_named, csv_path)   
    # save list of files that were combined and can be deleted
    combined_files_name = f'delete_combined_files_{u}.csv'
    combined_files_path = get_data_path(combined_files_name)
    save_list_to_csv(list_of_combined_files, combined_files_path)

    return csv_name, combined_files_name

def aggregate_data(ui_named):
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
            too_fine_list += too_fine
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
    too_fine_name = f'delete_too_fine_{u}.csv'
    too_fine_path = get_data_path(too_fine_name)
    save_csv(too_fine_list, too_fine_path)

    return too_fine_name

if __name__ == "__main__":
    all_named = []
    for i in config.UI_LIST:
        cur_ui = get_data_path(i)
        named = 'named_' + i
        failed = get_data_path('failed_' + i)

        all_named.append(named)

        download_data(cur_ui, named, failed)

    print(f"Combining files if longitude values are consecutive.")
    grp_cols = ['start_time', 'end_time', 'temporal_resolution', 'spatial_resolution', 'max_lat_N', 'min_lat_S']
    sort_col = 'max_long_E'
    compared_col = 'min_long_W'
    merge_dim = 'longitude'
    combined_long, combined_files_long = combine_data(all_named=all_named, 
                                               all_named_together='downloaded_named.csv', 
                                               grp_cols=grp_cols, 
                                               sort_col=sort_col, 
                                               compared_col=compared_col, 
                                               merge_dim=merge_dim,
                                               t_bool=False)
    
    print(f"Combining files if time values are consecutive")
    grp_cols_t = ['temporal_resolution', 'spatial_resolution', 'max_lat_N', 'min_lat_S', 'max_long_E', 'min_long_W']
    sort_col_t = 'start_time'
    compared_col_t = 'end_time'
    merge_dim_t = config.TIME
    combined_time, combined_files_time = combine_data(all_named=[combined_long], 
                                               all_named_together=combined_long, 
                                               grp_cols=grp_cols_t, 
                                               sort_col=sort_col_t, 
                                               compared_col=compared_col_t, 
                                               merge_dim=merge_dim_t,
                                               t_bool=True)
    
    files_to_delete = aggregate_data(get_data_path(combined_time))

    if config.DELETE:
        print("Starting file deletion.")
        delete_files(filenames=combined_files_long, directory=config.CUR_DATA_D)    # deletes files that were combined in longitude dim
        delete_files(filenames=combined_files_time, directory=config.CUR_DATA_D)    # deletes files that were combined in temporal dim
        delete_files(filenames=files_to_delete, directory=config.CUR_DATA_D)  # deletes files that are too fine-grained

        print(f'All files in \n\t{combined_files_long}, \n\t{combined_files_time} and \n\t{files_to_delete} have been deleted.')
    else:
        print(f'Files to delete in \n\t{combined_files_long}, \n\t{combined_files_time} and \n\t{files_to_delete}.')
