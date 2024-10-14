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

def generate_year_range(start_year, end_year):
    '''
    IN: start_year, end_year (str?) - year range that need to be downloaded

    OUT: (list) - list of strings with range of all years needed
    '''
    return [str(i) for i in range(int(start_year), int(end_year) + 1)]

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
            start_year, end_year = row[col_name['s_y']], row[col_name['e_y']]
            # start_month, end_month = row[col_name['s_m']], row[col_name['e_m']]
            # start_day, end_day = row[col_name['s_d']], row[col_name['e_d']]
            # start_time, end_time = row[col_name['s_t']], row[col_name['e_t']]
            file_path = row[col_name['path']]

            # Create year, month, day, and time ranges
            year_range = generate_year_range(start_year, end_year)
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

def files_to_delete(input_csv, output_csv, resolution='temporal'):
    '''
    IN: input_csv (str) - file name of csv that has the initial user input

        output_csv (str) - file name of csv that will be created that will have file names of files to delete

        resolution (str) - type of resolution that is being pruned [default: 'temporal']
    '''
    with open(input_csv, mode='r') as file:
        reader = csv.DictReader(file)
        with open(output_csv, mode='w', newline='') as outfile:
            writer = csv.writer(outfile)
            writer.writerow(['id', 'file_names'])
            for row in reader:
                file_path = row['file_path']
                id_number = file_path.split('_')[-1].split('.')[0]
                file_names = []
                if resolution == 'spatial':
                    # Add file names based on the 'spatial_resolution' value
                    if row['spatial_resolution'] == '0.5':
                        file_names.append(f'agg_{id_number}_*_025.nc')
                    elif row['spatial_resolution'] == '1':
                        file_names.append(f'agg_{id_number}_*_025.nc')
                        file_names.append(f'agg_{id_number}_*_050.nc')
                else:
                    # Add file names based on the 'temporal_resolution' value
                    if row['temporal_resolution'] == 'day':
                        file_names.append(f'agg_{id_number}_hour.nc')
                    elif row['temporal_resolution'] == 'month':
                        file_names.append(f'agg_{id_number}_hour.nc')
                        file_names.append(f'agg_{id_number}_day.nc')
                    elif row['temporal_resolution'] == 'year':
                        file_names.append(f'agg_{id_number}_hour.nc')
                        file_names.append(f'agg_{id_number}_day.nc')
                        file_names.append(f'agg_{id_number}_month.nc')
                # Write the 'id' and the file names as a comma-separated string
                writer.writerow([id_number, ', '.join(file_names)])

def delete_files(names_csv, folder_path):
    '''
    IN: names_csv (str) - file name of csv that has the names of files to delete

        folder_path (str) - path to folder where files to be deleted can be found

    Deletes the files whose names are listed in the input_csv
    '''
    with open(names_csv, mode='r') as file:
        reader = csv.reader(file)
        next(reader)  # Skip the header row
        for row in reader:
            # The second column contains the file names as a comma-separated string
            file_names = row[1].split(', ')
            for file_name in file_names:
                file_pattern = os.path.join(folder_path, file_name)
                matching_files = glob.glob(file_pattern)
                for matched_file in matching_files:
                    try:
                        os.remove(matched_file)
                        print(f"Deleted: {matched_file}")
                    except OSError as e:
                        print(f"Error deleting {matched_file}: {e}")
                if not matching_files:
                    print(f"No files matched the pattern: {file_pattern}")
