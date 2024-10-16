'''
func.py

DESCRIPTION: Contains functions that are used in customized_storage.py

Author: Ana Uribe
'''

'''Imports'''
import pandas as pd
import csv
import cdsapi
import os
import glob

from utils.const import col_name, api_request_settings
from utils.temporal_aggregation import get_res_d, get_res_m, get_res_y
from utils.spatial_aggregation import get_res_050, get_res_100

def get_time_range_ids(df):
    '''
    IN: df (pandas data frame) - rows with start and end datetimes

    OUT: df (pandas data frame) - df with new range_id col
    '''
    s = col_name['start_t']
    e = col_name['end_t']
    r_id = col_name['t_r_id']

    df = df.sort_values(by=s).reset_index(drop=True)
    cur_end = df.iloc[0][e]
    cur_range_id = 1
    df[r_id] = cur_range_id

    for i in range(1, len(df)):
        next_start = df.iloc[i][s]
        next_end = df.iloc[i][e]

        if next_start <= cur_end:
            cur_end = max(cur_end, next_end)
            df.loc[i, r_id] = cur_range_id
        else:
            cur_range_id += 1
            cur_end = next_end
            df.loc[i, r_id] = cur_range_id

    return df

def get_location_range_ids(df):
    '''
    IN: df (pandas data frame) - rows with start and end datetimes, and range_id col

    OUT: df (pandas data frame) - df with new location_id col
    '''
    min_lat = col_name['min_lat']
    max_lat = col_name['max_lat']
    min_long = col_name['min_long']
    max_long = col_name['max_long']

    # TODO: rewrite with groupby and apply 
    unique_time_ranges = df[col_name['t_r_id']].unique()
    for r in unique_time_ranges:

        t_range_df = df[df[col_name['t_r_id']] == r]

def get_year_range(start_year, end_year):
    '''
    IN: start_year, end_year (str?) - year range that need to be downloaded

    OUT: (list) - list of strings with range of all years needed
    '''
    return [str(i) for i in range(int(start_year), int(end_year) + 1)]

def get_list_of_files_in_folder(folder_path):
    '''
    IN: folder_path (str) - path to folder where you want all your 

    OUT: list_of_files (list) - list of all the file paths (full paths) in the folder
    '''
    list_of_files = []
    for file_name in os.listdir(folder_path):
        file_path = os.path.join(folder_path, file_name)
        if os.path.isfile(file_path):
            list_of_files.append()
    
    return list_of_files

def download_data_from_csv(input_csv):
    '''
    IN: input_csv (str) - file name of csv that has the initial user input

    An API call to CDS is created for each row in input_csv and the corresponding data is downloaded
    '''
    with open(input_csv, 'r') as file:
        reader = csv.DictReader(file)
        for row in reader:
            variable = row[col_name['var']]
            max_lat, min_lat = row[col_name['max_lat']], row[col_name['min_lat']]
            max_long, min_long = row[col_name['max_long']], row[col_name['min_long']]
            
            # v0: years in v0_user_input.csv are in cols start_time and end_time
            start_year, end_year = row[col_name['s_t']], row[col_name['e_t']]

            # start_year, end_year = row[col_name['s_y']], row[col_name['e_y']]
            # start_month, end_month = row[col_name['s_m']], row[col_name['e_m']]
            # start_day, end_day = row[col_name['s_d']], row[col_name['e_d']]
            # start_time, end_time = row[col_name['s_t']], row[col_name['e_t']]
            file_path = row[col_name['path']]

            # Create year, month, day, and time ranges
            year_range = get_year_range(start_year, end_year)
            month_range = api_request_settings['month']
            day_range = api_request_settings['day']
            time_range = api_request_settings['time']

            # Set up the API client and request
            dataset = api_request_settings['dataset']
            request = {
                'product_type': api_request_settings['product_type'],
                'variable': [variable],
                'year': year_range,
                'month': month_range,
                'day': day_range,
                'time': time_range,
                'data_format': api_request_settings['data_format'],
                'download_format': api_request_settings['download_format'],
                'area': [max_lat, min_long, min_lat, max_long]  # Order: [N, W, S, E]
            }
            
            client = cdsapi.Client()
            
            # Download the data
            print(f"Downloading {variable} data for {start_year}-{end_year} to {file_path}")

            # client.retrieve(dataset, request).download(file_path)   # have to click download button on website
            client.retrieve(dataset, request, file_path)

def files_to_delete(input_csv, output_csv, output_folder, resolution='temporal'):
    '''
    IN: input_csv (str) - file name of csv that has the initial user input

        output_csv (str) - file name of csv that will be created that will have file names of files to delete

        output_folder (str) - path to folder where files to delete will be

        resolution (str) - type of resolution that is being pruned [default: 'temporal']
    '''
    with open(input_csv, mode='r') as file:
        reader = csv.DictReader(file)
        with open(output_csv, mode='w', newline='') as outfile:
            writer = csv.writer(outfile)
            writer.writerow(['id', 'file_names'])
            for row in reader:
                file_path = row[col_name['path']]
                id_number = file_path.split('_')[-1].split('.')[0]
                file_names = []

                if resolution == 'spatial':
                    # Add file names based on the 'spatial_resolution' value
                    if row[col_name['s_res']] == '0.5':
                        file_names.append(os.path.join(output_folder, f'agg_{id_number}_*_025.nc'))
                    elif row[col_name['s_res']] == '1':
                        file_names.append(os.path.join(output_folder, f'agg_{id_number}_*_025.nc'))
                        file_names.append(os.path.join(output_folder, f'agg_{id_number}_*_050.nc'))
                else:
                    # Add file names based on the 'temporal_resolution' value
                    if row[col_name['t_res']] == 'day':
                        file_names.append(os.path.join(output_folder, f'agg_{id_number}_hour.nc'))
                    elif row[col_name['t_res']] == 'month':
                        file_names.append(os.path.join(output_folder, f'agg_{id_number}_hour.nc'))
                        file_names.append(os.path.join(output_folder, f'agg_{id_number}_day.nc'))
                    elif row[col_name['t_res']] == 'year':
                        file_names.append(os.path.join(output_folder, f'agg_{id_number}_hour.nc'))
                        file_names.append(os.path.join(output_folder, f'agg_{id_number}_day.nc'))
                        file_names.append(os.path.join(output_folder, f'agg_{id_number}_month.nc'))

                # Write the 'id' and the file names as a comma-separated string
                writer.writerow([id_number, ', '.join(file_names)])

def delete_files(files_to_delete):
    '''
    IN: files_to_delete (list) - list of files (with their full path) to try to delete

    Deletes files whose file paths are in files_to_delete list
    '''
    for file in files_to_delete:
        matching_files = glob.glob(file)
        if not matching_files:
            print(f"No files matched the pattern: {file}")
        else:
            for match in matching_files:
                try:
                    os.remove(match)
                    print(f"Deleted: {match}")
                except OSError as e:
                    print(f"Error deleting {match}: {e}")

def temporal_aggregation(input_csv, input_folder_path, output_folder_path):
    '''
    IN: input_csv (str) - file name of csv that has the initial user input

        input_folder_path (str) - folder where the raw files are stored

        output_folder_path (str) - folder where new files will be put

    For every row in input_csv file, get file name 'original_file_name' and aggregate data in this file.
    For each aggregation (temporal aggregation={day, month, year}), aggregate the corresp. file and save to
    a new file with name pattern: 'agg_<id_number>_<temporal aggregation>.nc' (where id_number comes from original_file_name)
    '''
    with open(input_csv, 'r') as file:
        reader = csv.DictReader(file)
        for row in reader:
            original_file_name = row[col_name['path']]
            finest_file_path = os.path.join(input_folder_path, original_file_name)
            # assuming original_file_name has 'raw_<id_number>.nc' structure
            id_number = original_file_name.split('_')[-1].split('.')[0]

            file_d = os.path.join(output_folder_path, f'agg_{id_number}_day.nc')    # new file name
            get_res_d(finest_file_path, file_d)
            print(f'Aggregated data from {original_file_name} into daily resolution.\nSaving to {file_d}.')

            file_m = os.path.join(output_folder_path, f'agg_{id_number}_month.nc')
            get_res_m(file_d, file_m)
            print(f'Aggregated data from {original_file_name} into monthly resolution.\nSaving to {file_m}.')

            file_y = os.path.join(output_folder_path, f'agg_{id_number}_year.nc')
            get_res_y(file_m, file_y)
            print(f'Aggregated data from {original_file_name} into yearly resolution.\nSaving to {file_y}.')

def spatial_aggregation(folder_path):
    '''
    IN: input_csv (str) - file name of csv that has the initial user input

        folder_path (str) - folder where temporally aggregated files are stored

    For all file_paths in folder, for each aggregation (spatial aggregation ={0.5, 1}), aggregate the file and save to
    a new file with name pattern: 'agg_<id_number>_<temporal aggregation>_<spatial aggregation>.nc' (where id_number comes from file_path)
    '''
    # TODO: decide if putting all files (raw and agg) in same folder or if going thorugh next loop for the raw folder and the agg folder.
    temporal_files = get_list_of_files_in_folder(folder_path)   
    for file_path in temporal_files:
        # assuming file path has '*/*_<id_number>_<temporal_aggregation>.nc' structure
        id_number = file_path.split('_')[-2]
        temporal_aggregation = file_path.split('_')[-1].split('.')[0]

        file_050 = os.path.join(folder_path, f'agg_{id_number}_{temporal_aggregation}_050.nc')
        get_res_050(file_path, file_050)
        print(f'Aggregated data form {file_path} into 0.5 degree spatial resolution.\nSaving to {file_050}.')

        file_100 = os.path.join(folder_path, f'agg_{id_number}_{temporal_aggregation}_100.nc')
        get_res_100(file_path, file_100)
        print(f'Aggregated data form {file_path} into 1.0 degree spatial resolution.\nSaving to {file_100}.')