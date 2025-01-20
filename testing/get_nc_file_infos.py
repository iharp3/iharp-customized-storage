import os
import xarray as xr
import pandas as pd
import numpy as np
import glob
import math
import re

VAR_SHORT_N = {"2m_temperature":"t2m",
               "total_precipitation":"tp",
               "snow_depth": "snow_depth",
               "sea_surface_temperature": "sst",
               "10m_u_component_of_wind": "u10",
               "10m_v_component_of_wind": "v10",
               "surface_pressure": "sp"}

def get_file_size(file_path):
    file_size_bytes = os.path.getsize(file_path)
    file_size_mb = file_size_bytes / (1024 * 1024)

    return round(file_size_mb, 2)

def get_min_max_of_array(arr):
    v_min = arr.min().compute().values.item()
    v_max = arr.max().compute().values.item()
    # print(f"\tMin:{v_min}, Max:{v_max}")

    return round(v_min, 2), round(v_max, 2)

def calculate_temporal_resolution(valid_time):
    # Compute the differences between consecutive time values
    time_diffs = valid_time[1:] - valid_time[:-1]

    # Display the unique differences
    unique_diffs = pd.Series(time_diffs).value_counts()
    # print("Unique time differences and their counts:")
    # print(unique_diffs)
    
    # Classify the time differences
    classifications = []
    t = "NULL"
    # i = 0
    for diff in time_diffs:
        # if i%10==0:
            # print(f"\ni: {i}\tdif{diff}")
            # print(f"\npd.Timedelta(hours=1): {pd.Timedelta(hours=1)}")
        # i+=1

        if diff >= pd.Timedelta(hours=0.5) and diff <= pd.Timedelta(hours=5):
            classifications.append("H")
            t = "H"
        elif diff >= pd.Timedelta(days=0.5) and diff <= pd.Timedelta(days=3):
            classifications.append("D")
            t = "D"
        elif diff >= pd.Timedelta(days=27) and diff < pd.Timedelta(days=32):
            classifications.append("M")
            t = "M"
        elif diff >= pd.Timedelta(days=364) and diff <= pd.Timedelta(days=367):
            classifications.append("Y")
            t = "Y"
        else:
            classifications.append(f"{diff}")
            t = "NULL"

        # if i%10==0:
        #     print(f"\nclassifications list: {classifications}")
        #     print(f"\nt: {t}")

    if len(classifications) > 0:
        return classifications[0]
    else:
        return t

def calculate_spatial_resolution(latitude):
    # Compute the differences between consecutive latitude values
    lat_diffs = np.diff(latitude)
    
    # Check for uniform spacing
    # if np.allclose(lat_diffs, lat_diffs[0]):
        # print(f"Spatial resolution (latitude): {lat_diffs[0]:.6f} degrees")
    # else:
        # print("Spatial resolution is not uniform.")
        # print(f"Unique latitude differences: {np.unique(lat_diffs)}")

    return abs(lat_diffs[0])

def make_list_of_files(directory, var_name, token, n):
    # Define the pattern
    pattern = os.path.join(directory, var_name + "_t*_s*_" + token + "_" + n + "*.nc")


    # Get the list of matching files
    file_list = glob.glob(pattern)

    return file_list

def get_agg_types(f, t_res):

    pattern = r"_(mean|max|min)_(mean|max|min)\.nc$"
    match = re.search(pattern, f)
    if match:
        t_agg, s_agg = match.groups()
    else:

        pattern = r"_(mean|max|min).nc$"
        match = re.search(pattern, f)
        if match:
            if t_res == "H":
                t_agg = "none"
                s_agg = match.group(1)
            else:
                t_agg = match.group(1)
                s_agg = "none"
        else:
            t_agg = s_agg = "none"

    return t_agg, s_agg


def extract_netcdf_metadata_from_list(need_metadata_list, var_name, folder_path, output_csv):
    """
    Extract metadata from all NetCDF files in a folder and save the results to a CSV file.

    Parameters:
        output_csv (str): Path to save the output CSV file.
    """
    # List to store metadata for each file
    metadata = []

    # Iterate through all files in the folder
    for filename in need_metadata_list:
        file_path = os.path.join(folder_path, filename)
        try:
            # get file size
            file_size = get_file_size(file_path=file_path)
            # Open the NetCDF file using xarray
            with xr.open_dataset(file_path) as ds:
                file_name = filename
                # Extract valid_time dimension range
                if 'valid_time' in ds:
                    valid_time = pd.to_datetime(ds['valid_time'].values)
                    valid_time_start = valid_time.min().strftime('%Y-%m-%dT%H:%M')
                    valid_time_end = valid_time.max().strftime('%Y-%m-%dT%H:%M')
                    t_res = calculate_temporal_resolution(valid_time)
                else:
                    valid_time_start = valid_time_end = t_res = None

                # Extract latitude range
                if 'latitude' in ds:
                    latitude_min = math.floor(float(ds['latitude'].min()))
                    latitude_max = math.ceil(float(ds['latitude'].max()))
                    latitude = ds['latitude'].values
                    s_res = calculate_spatial_resolution(latitude)
                else:
                    latitude_min = latitude_max = s_res = None

                # Extract longitude range
                if 'longitude' in ds:
                    longitude_min = math.floor(float(ds['longitude'].min()))
                    longitude_max = math.ceil(float(ds['longitude'].max()))
                else:
                    longitude_min = longitude_max = None

                # Extract min and max of values
                v_min, v_max = get_min_max_of_array(arr=ds[VAR_SHORT_N[var_name]])

                # Get aggregation (mean, min, max) type
                t_agg, s_agg = get_agg_types(file_name, t_res=t_res)

                # Append the metadata to the list
                metadata.append({
                    'variable': var_name,
                    'max_lat_N': latitude_max,
                    'min_lat_S': latitude_min,
                    'max_long_E': longitude_max,
                    'min_long_W': longitude_min,
                    'start_time': valid_time_start,
                    'end_time': valid_time_end,
                    'temporal_resolution': t_res,
                    'spatial_resolution': s_res,
                    'temporal_agg_type': t_agg,
                    'spatial_agg_type': s_agg,
                    'min': v_min,
                    'max': v_max,
                    'file_size': file_size,
                    'file_name': file_name
                })
        except Exception as e:
            print(f"Error processing file {filename}: {e}")

    # Create a DataFrame from the metadata
    metadata_df = pd.DataFrame(metadata)

    # Save the DataFrame to a CSV file
    metadata_df.to_csv(output_csv, index=False)


# Get info for sea_surface_temp_N that didn't save :/
var_name = 'sea_surface_temperature'
folder_path = '/data/iharp-customized-storage/storage/sea_surface_temperature_N'
output_csv = '/data/iharp-customized-storage/testing/' + var_name + '_meta.csv'

# Get the list of files that don't have metadata:
num ="1736979894617"
need_metadata_list = make_list_of_files(directory=folder_path, var_name=var_name, token="Y", n=num)
extract_netcdf_metadata_from_list(need_metadata_list, var_name, folder_path, output_csv)

# def extract_netcdf_metadata(folder_path, output_csv):
#     """
#     Extract metadata from all NetCDF files in a folder and save the results to a CSV file.

#     Parameters:
#         folder_path (str): Path to the folder containing NetCDF files.
#         output_csv (str): Path to save the output CSV file.
#     """
#     # List to store metadata for each file
#     metadata = []

#     # Iterate through all files in the folder
#     for filename in os.listdir(folder_path):
#         if filename.endswith('.nc'):
#             file_path = os.path.join(folder_path, filename)
#             try:
#                 # Open the NetCDF file using xarray
#                 with xr.open_dataset(file_path) as ds:
#                     # Extract metadata
#                     file_name = filename

#                     # Extract valid_time dimension range
#                     if 'valid_time' in ds:
#                         valid_time = pd.to_datetime(ds['valid_time'].values)
#                         valid_time_start = valid_time.min()
#                         valid_time_end = valid_time.max()
#                     else:
#                         valid_time_start = valid_time_end = None

#                     # Extract latitude range
#                     if 'latitude' in ds:
#                         latitude_min = float(ds['latitude'].min())
#                         latitude_max = float(ds['latitude'].max())
#                     else:
#                         latitude_min = latitude_max = None

#                     # Extract longitude range
#                     if 'longitude' in ds:
#                         longitude_min = float(ds['longitude'].min())
#                         longitude_max = float(ds['longitude'].max())
#                     else:
#                         longitude_min = longitude_max = None

#                     # Append the metadata to the list
#                     metadata.append({
#                         'File Name': file_name,
#                         'Valid Time Start': valid_time_start,
#                         'Valid Time End': valid_time_end,
#                         'Latitude Min': latitude_min,
#                         'Latitude Max': latitude_max,
#                         'Longitude Min': longitude_min,
#                         'Longitude Max': longitude_max
#                     })
#             except Exception as e:
#                 print(f"Error processing file {filename}: {e}")

#     # Create a DataFrame from the metadata
#     metadata_df = pd.DataFrame(metadata)

#     # Save the DataFrame to a CSV file
#     metadata_df.to_csv(output_csv, index=False)


# def extract_some_netcdf_metadata(file_names_list, output_csv):
#     """
#     Extract metadata from all NetCDF files in a folder and save the results to a CSV file.

#     Parameters:

#         output_csv (str): Path to save the output CSV file.
#     """
#     # List to store metadata for each file
#     metadata = []

#     # Iterate through all files in the folder
#     for filename in file_names_list:
#         try:
#             file_path = os.path.join("/data/iharp-customized-storage/storage/temperature/", filename)
#             # Open the NetCDF file using xarray
#             with xr.open_dataset(file_path) as ds:
#                 # Extract metadata
#                 file_name = filename

#                 # Extract valid_time dimension range
#                 if 'valid_time' in ds:
#                     valid_time = pd.to_datetime(ds['valid_time'].values)
#                     valid_time_start = valid_time.min()
#                     valid_time_end = valid_time.max()
#                 else:
#                     valid_time_start = valid_time_end = None

#                 # Extract latitude range
#                 if 'latitude' in ds:
#                     latitude_min = float(ds['latitude'].min())
#                     latitude_max = float(ds['latitude'].max())
#                 else:
#                     latitude_min = latitude_max = None

#                 # Extract longitude range
#                 if 'longitude' in ds:
#                     longitude_min = float(ds['longitude'].min())
#                     longitude_max = float(ds['longitude'].max())
#                 else:
#                     longitude_min = longitude_max = None

#                 # Append the metadata to the list
#                 metadata.append({
#                     'File Name': file_name,
#                     'Valid Time Start': valid_time_start,
#                     'Valid Time End': valid_time_end,
#                     'Latitude Min': latitude_min,
#                     'Latitude Max': latitude_max,
#                     'Longitude Min': longitude_min,
#                     'Longitude Max': longitude_max
#                 })
#         except Exception as e:
#             print(f"Error processing file {filename}: {e}")

#     # Create a DataFrame from the metadata
#     metadata_df = pd.DataFrame(metadata)

#     # Save the DataFrame to a CSV file
#     metadata_df.to_csv(output_csv, index=False)

# # Example usage
# # folder_path = "/home/uribe055/iharp-customized-storage/storage"
# # output_csv = "file_info_output.csv"
# # extract_netcdf_metadata(folder_path, output_csv)

# # Example usage
# files_list = ['2m_temperature_t0_s0_1736344205223.nc', '2m_temperature_t0_s0_1736319787894.nc']
# output_csv = "ui_1_file_info_output.csv"
# extract_some_netcdf_metadata(files_list, output_csv)