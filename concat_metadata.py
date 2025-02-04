import pandas as pd
import argparse
import os

"""
python concat_metadata.py -S /data/iharp-customized-storage/storage
"""
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-S", "--storage_folder", type=str, help="Path to storage folder.")
    args = parser.parse_args()
    storage_folder = args.storage_folder

    metadata_list = []

    # for all folders in the storage folder
    for folder in os.scandir(storage_folder):
        if os.path.isdir(folder):
            metadata = folder.path + "/0_post_metadata.csv"
            # if metadata file exists
            if os.path.exists(metadata):
                print(metadata)
                df = pd.read_csv(metadata)
                print(len(df))
                metadata_list.append(df)

    # concatenate all metadata files
    df_meta = pd.concat(metadata_list)
    print(len(df_meta))
    df_meta.to_csv("website_metadata_do_not_delete.csv", index=False)
