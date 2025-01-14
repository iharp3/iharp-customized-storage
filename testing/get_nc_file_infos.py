import os
import xarray as xr
import pandas as pd

def extract_netcdf_metadata(folder_path, output_csv):
    """
    Extract metadata from all NetCDF files in a folder and save the results to a CSV file.

    Parameters:
        folder_path (str): Path to the folder containing NetCDF files.
        output_csv (str): Path to save the output CSV file.
    """
    # List to store metadata for each file
    metadata = []

    # Iterate through all files in the folder
    for filename in os.listdir(folder_path):
        if filename.endswith('.nc'):
            file_path = os.path.join(folder_path, filename)
            try:
                # Open the NetCDF file using xarray
                with xr.open_dataset(file_path) as ds:
                    # Extract metadata
                    file_name = filename

                    # Extract valid_time dimension range
                    if 'valid_time' in ds:
                        valid_time = pd.to_datetime(ds['valid_time'].values)
                        valid_time_start = valid_time.min()
                        valid_time_end = valid_time.max()
                    else:
                        valid_time_start = valid_time_end = None

                    # Extract latitude range
                    if 'latitude' in ds:
                        latitude_min = float(ds['latitude'].min())
                        latitude_max = float(ds['latitude'].max())
                    else:
                        latitude_min = latitude_max = None

                    # Extract longitude range
                    if 'longitude' in ds:
                        longitude_min = float(ds['longitude'].min())
                        longitude_max = float(ds['longitude'].max())
                    else:
                        longitude_min = longitude_max = None

                    # Append the metadata to the list
                    metadata.append({
                        'File Name': file_name,
                        'Valid Time Start': valid_time_start,
                        'Valid Time End': valid_time_end,
                        'Latitude Min': latitude_min,
                        'Latitude Max': latitude_max,
                        'Longitude Min': longitude_min,
                        'Longitude Max': longitude_max
                    })
            except Exception as e:
                print(f"Error processing file {filename}: {e}")

    # Create a DataFrame from the metadata
    metadata_df = pd.DataFrame(metadata)

    # Save the DataFrame to a CSV file
    metadata_df.to_csv(output_csv, index=False)


def extract_some_netcdf_metadata(file_names_list, output_csv):
    """
    Extract metadata from all NetCDF files in a folder and save the results to a CSV file.

    Parameters:

        output_csv (str): Path to save the output CSV file.
    """
    # List to store metadata for each file
    metadata = []

    # Iterate through all files in the folder
    for filename in file_names_list:
        try:
            file_path = os.path.join("/data/iharp-customized-storage/storage/temperature/", filename)
            # Open the NetCDF file using xarray
            with xr.open_dataset(file_path) as ds:
                # Extract metadata
                file_name = filename

                # Extract valid_time dimension range
                if 'valid_time' in ds:
                    valid_time = pd.to_datetime(ds['valid_time'].values)
                    valid_time_start = valid_time.min()
                    valid_time_end = valid_time.max()
                else:
                    valid_time_start = valid_time_end = None

                # Extract latitude range
                if 'latitude' in ds:
                    latitude_min = float(ds['latitude'].min())
                    latitude_max = float(ds['latitude'].max())
                else:
                    latitude_min = latitude_max = None

                # Extract longitude range
                if 'longitude' in ds:
                    longitude_min = float(ds['longitude'].min())
                    longitude_max = float(ds['longitude'].max())
                else:
                    longitude_min = longitude_max = None

                # Append the metadata to the list
                metadata.append({
                    'File Name': file_name,
                    'Valid Time Start': valid_time_start,
                    'Valid Time End': valid_time_end,
                    'Latitude Min': latitude_min,
                    'Latitude Max': latitude_max,
                    'Longitude Min': longitude_min,
                    'Longitude Max': longitude_max
                })
        except Exception as e:
            print(f"Error processing file {filename}: {e}")

    # Create a DataFrame from the metadata
    metadata_df = pd.DataFrame(metadata)

    # Save the DataFrame to a CSV file
    metadata_df.to_csv(output_csv, index=False)

# Example usage
# folder_path = "/home/uribe055/iharp-customized-storage/storage"
# output_csv = "file_info_output.csv"
# extract_netcdf_metadata(folder_path, output_csv)

# Example usage
files_list = ['2m_temperature_t0_s0_1736344205223.nc', '2m_temperature_t0_s0_1736319787894.nc']
output_csv = "ui_1_file_info_output.csv"
extract_some_netcdf_metadata(files_list, output_csv)