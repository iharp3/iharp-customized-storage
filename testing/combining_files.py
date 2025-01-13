import os
import pandas as pd
import csv
import time

def get_unique_num():
    return int(time.time() * 1000)

def get_raw_file_name(var):
    """
    Creates file name with unique number based on time.
    """
    u = get_unique_num()
    file_name = f"{var}_t0_s0_{u}.nc"

    return file_name

def save_csv(rows, file_path):
    """
    Saves csv file from list of dictionaries. Keys become column names in header.
    """
    headers = rows[0].keys() if rows else []
    with open(file_path, 'w', newline='') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=headers)
        writer.writeheader()
        writer.writerows(rows)

def combine_data(all_named, out):
    df = pd.concat([pd.read_csv(file) for file in all_named], ignore_index=True)
    group_cols = ['start_time', 'end_time', 'temporal_resolution', 'spatial_resolution', 'max_lat_N', 'min_lat_S']
    grouped = df.groupby(group_cols)

    final_ui_named = []
    list_of_combined_files = []
    # u = get_unique_num()
    u = 167

    for _, group in grouped:
        if len(group) == 1:
            d = group.iloc[0].to_dict()
            final_ui_named.append(d)
        else:
            # order max_long_E
            sorted_group = group.sort_values(by='max_long_E', ascending=False)
            # check if consecutive
            consecutive_files = []
            final_min_long_W = -300
            count = 0
            for i in range(1, len(sorted_group)):
                count = i
                # print(f'\ni={i}')
                prev_row = sorted_group.iloc[i-1].to_dict()
                cur_row = sorted_group.iloc[i].to_dict()
                # rows are consecutive
                if prev_row['min_long_W'] == cur_row['max_long_E']:
                    if (i-1) == 0:
                        consecutive_files.append(prev_row['file_name'])
                    consecutive_files.append(cur_row['file_name'])
                    final_min_long_W = cur_row['min_long_W']
                    print(final_min_long_W)
                # rows stop being consecutive
                else:
                    break
                    # TODO: make it so the second and third rows could be merged rather than just the first + others
            if final_min_long_W != -300:
                # combine consecutive files
                file_paths = consecutive_files  # TODO: check what final file_names are [os.path.join(config.CUR_DATA_D, file) for file in consecutive_files]
                
                # datasets = [xr.open_dataset(file) for file in file_paths]
                # merged_dataset = xr.concat(datasets, dim='longitude')
                # for ds in datasets:
                #     ds.close()

                # # save concatenation to new file
                merged_dataset_name = get_raw_file_name(sorted_group.iloc[0]['variable'])
                # merged_dataset.to_netcdf(os.path.join(out, merged_dataset_name))

                # add row to final_ui_named
                merged_row = sorted_group.iloc[0].to_dict()
                # if final_min_long_W != -300:
                    # print(final_min_long_W)
                print(f'final:{final_min_long_W}\n')
                merged_row['min_long_W'] = final_min_long_W
                merged_row['file_name'] = merged_dataset_name
                # print(merged_row)
                final_ui_named.append(merged_row)

                list_of_combined_files += consecutive_files

            # print(f'prev_row[W]: {prev_row['min_long_W']} \t cur_row[E]: {cur_row['max_long_E']}')
            if count < len(sorted_group):
                # add the remaining files to final_ui_named
                for j in range(len(sorted_group) - count):
                    final_ui_named.append(sorted_group.iloc[j + count].to_dict())

            # update final_ui_named csv with each group
            csv_name = sorted_group.iloc[0]['variable'] + f'_final_user_interest_{u}.csv'
            csv_path = os.path.join(out, csv_name)
            save_csv(final_ui_named, csv_path)
        
    # save list of files that were combined and can be deleted
    combined_files_name = sorted_group.iloc[0]['variable'] + f'delete_combined_files_{u}.csv'
    combined_files_path = os.path.join(out, combined_files_name)
    save_csv(list_of_combined_files, combined_files_path)

    return
'''
def combine_data(all_named, folder):
    """TODO: Assumes the variable column is the same for all rows"""
    df = pd.concat([pd.read_csv(file) for file in all_named], ignore_index=True)
    same_start_time = df.groupby(['start_time'])
    group_cols = ['start_time', 'end_time', 'temporal_resolution', 'spatial_resolution', 'max_lat_N', 'min_lat_S']
    
    grouped = df.groupby(group_cols)
    # print(grouped)

    final_ui_named = [] # csv with condensed user interest files
    list_of_combined_files = []
    # u = get_unique_num()
    u = '0167'
    for _, group in grouped:
        print(group)
        sorted_group = group.sort_values(by='max_long_E')
        # print(len(sorted_group))
        consecutive_files = []
        final_min_long_W = 0

        i = 0
        for i in range(len(sorted_group)):
            print('\nCONSECUTIVE FILES:\n')
            if i == 0:
                consecutive_files.append(sorted_group.iloc[i]['file_name'])
                print(f'\n\ti={i}:\n\t{consecutive_files}')
            else:
                prev_row = sorted_group.iloc[i-1]
                cur_row = sorted_group.iloc[i]
                if prev_row['min_long_W'] == cur_row['max_long_E']:
                    consecutive_files.append(cur_row['file_name'])
                    print(f'\n\ti={i}:\n{consecutive_files}')
                    final_min_long_W = cur_row['min_long_W']
                else:
                    # add the remaining files to final_ui_named
                    for j in range(len(sorted_group) - i):
                        final_ui_named.append(sorted_group.iloc[j + i])
                        print(f'\n\t\tFINAL_UI: {final_ui_named}')
                    break
        print(f'length of consecutive files: {len(consecutive_files)}')
        if len(consecutive_files) == 1:     # no consecutive files, add first row to final_ui_named
            print('sorted_group.iloc[0]')
            print(sorted_group.iloc[0])
            final_ui_named.append(sorted_group.iloc[0])
            print(f'final_ui_named: {final_ui_named}')
        else:
            # concatenate consecutive files
            file_paths = [os.path.join(config.CUR_DATA_D, file) for file in consecutive_files]
            print('\nFile Paths:\n\t')
            # datasets = [xr.open_dataset(file) for file in file_paths]
            for file in file_paths:
                print(file)
                if os.path.isfile(file):
                    print('\tfile is a file')
            # merged_dataset = xr.concat(datasets, dim='longitude')
            # for ds in datasets:
            #     ds.close()
            # save concatenation to new file
            merged_dataset_name = get_raw_file_name(sorted_group.iloc[0]['variable'])
            # merged_dataset.to_netcdf(merged_dataset_name)
            # add row to final_ui_named
            merged_row = sorted_group.iloc[0]
            merged_row['min_long_W'] = final_min_long_W
            merged_row['file_name'] = merged_dataset_name
            final_ui_named.append(merged_row)

        print('WHAT')
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
'''    
combine_data(['/home/uribe055/iharp-customized-storage/testing/named_ui_2_temp.csv'], '/home/uribe055/iharp-customized-storage/testing/')