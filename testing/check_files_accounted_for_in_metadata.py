import csv
import pandas as pd
import os

checking = True
deleting = False


data_folder = "/data/iharp-customized-storage/storage/temperature/"
# df_meta = pd.read_csv(os.path.join("metadata.csv"))
df_meta = pd.read_csv('/data/iharp-customized-storage/storage/temperature/metadata.csv')

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
            if checking:
                print(f"\n{file} not in metadata")
            if deleting:
                file_path = os.path.join(data_folder, file)
                if os.path.exists(file_path):
                    os.remove(file_path)
                    print(f"{file} has been deleted.")

print(f"\nTotal .nc files: {nc_file_count}\nTotal .nc files in metadata.csv: {nc_files_in_meta_count}\nDifference: {abs(nc_file_count - nc_files_in_meta_count)}\n")
if not deleting:
    print(f"\nFiles not in metadata.csv were not deleted.\nChange 'deleting' variable to 'TRUE' and run script again to delete them.\n")