import csv
import pandas as pd
import os

data_folder = "/data/iharp-customized-storage/storage/sea_surface_temperature_N/"
df_meta = pd.read_csv(os.path.join("metadata.csv"))

meta_file_names = df_meta["file_name"]
meta_file_names_list = meta_file_names.to_list()

nc_file_count = 0
nc_files_in_meta_count = 0

for file in os.listdir(data_folder):
    if file.endswith(".nc"):
        nc_file_count += 1
        if file in meta_file_names_list:
            nc_files_in_meta_count += 1
        else:
            print(f"\n{file}")

print(f"\nTotal .nc files: {nc_file_count}\nTotal .nc files in metadata.csv: {nc_files_in_meta_count}\nDifference: {abs(nc_file_count - nc_files_in_meta_count)}\n")