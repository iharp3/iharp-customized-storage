import csv
import pandas as pd
import os

from get_nc_file_infos import extract_netcdf_metadata_from_list

# for each file in folder temperature
# if file name in metadata.csv [file_name]
# pass
# else get the metadata for it

# count to make sure it aligns with Yuchuan's

data_folder = "/data/iharp-customized-storage/storage/temperature/"
df_meta = pd.read_csv("/data/iharp-customized-storage/storage/temperature/metadata.csv")
nc_count = 0

for file in os.listdir(data_folder):
    full_path = data_folder + file
    if file.endswith(".nc"):
        nc_count += 1
        if file in df_meta["file_name"]:
            pass
        else:
            extract_netcdf_metadata_from_list([file], "2m_temperature", data_folder, "missing_metadata_2m_temp")

