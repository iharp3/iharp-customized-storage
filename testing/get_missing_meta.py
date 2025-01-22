import csv
import pandas as pd
import os

from get_nc_file_infos import extract_netcdf_metadata_from_list

# for each file in folder temperature
# if file name in metadata.csv [file_name]
# pass
# else get the metadata for it

# count to make sure it aligns with Yuchuan's

data_folder = "/data/iharp-customized-storage/storage/sea_surface_temperature_N/"
df_meta = pd.read_csv("/data/iharp-customized-storage/storage/sea_surface_temperature_N/metadata.csv")
meta_file_names = df_meta["file_name"]
meta_file_names_list = meta_file_names.to_list()
nc_count = 0
in_meta_count = 0
# print(meta_file_names_list)
# print(meta_file_names_list[0])
# print(df_meta['file_name'])
# exit

for file in os.listdir(data_folder):
    # full_path = data_folder + file
    # print(file)
    # print(df_meta['file_name'][0])
    # print(type(file) == type(df_meta["file_name"][0]))
    if file.endswith(".nc"):
        # print(type(file))
        # print(type(df_meta["file_name"][0]))
        nc_count += 1
        if file in meta_file_names_list:
            in_meta_count += 1
        else:
            # pass 
            print(f"\n{nc_count}: {file}")
            # extract_netcdf_metadata_from_list([file], "2m_temperature", data_folder, "missing_metadata_2m_temp")

print(f"\nTotal .nc files: {nc_count}\nTotal .nc files in metadata.csv: {in_meta_count}\n")
