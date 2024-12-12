"""
    PostProcess.py
    This is a temporary workaround to have the query executors and website working on the customized storage. 
    Data file post-process:
        - rename coordinates: valid_time -> time
        - drop coordinates: number, expver
    Metadata file post-process:
        - add column file_path (dir + file_name)
        - rename columns {
                "max_lat_N": "max_lat",
                "min_lat_S": "min_lat",
                "max_long_E": "max_lon",
                "min_long_W": "min_lon",
                "start_time": "start_datetime",
                "end_time": "end_datetime",
                "temporal_agg_type": "temporal_aggregation",
                "spatial_agg_type": "spatial_aggregation",
            }
        - convert temporal_resolution literal {"H": "hour", "D": "day", "M": "month", "Y": "year"}
"""

import argparse
import os
import xarray as xr
import pandas as pd

if __name__ == "__main__":
    # Example: python PostProcess.py -D /data/iharp-customized-storage/storage/ -M /data/iharp-customized-storage/storage/metadata.csv
    parser = argparse.ArgumentParser()
    parser.add_argument("-D", "--data_folder", type=str, help="Path to the data files folder.")
    parser.add_argument("-M", "--metadata", type=str, help="Path to the metadata file.")
    args = parser.parse_args()
    data_folder = args.data_folder
    metadata = args.metadata

    # make new directory for post-processed files
    destination = data_folder + "post/"
    if not os.path.exists(destination):
        os.makedirs(destination)

    # 1. data files
    print("Post-processing data files...")
    for file in os.listdir(data_folder):
        full_path = data_folder + file
        print(full_path)
        if file.endswith(".nc"):
            ds = xr.open_dataset(full_path)
            ds = ds.rename({"valid_time": "time"})
            if "number" in ds.coords:
                ds = ds.drop_vars("number")
            if "expver" in ds.coords:
                ds = ds.drop_vars("expver")
            ds.to_netcdf(destination + file.replace(".nc", "_post.nc"))
            # os.remove(full_path)  # remove the original file, disabled for now

    # 2. metadata file
    print(f"Post-processing metadata: {metadata}...")
    df_meta = pd.read_csv(metadata)
    # add columns
    df_meta["file_name"] = df_meta["file_name"].str.replace(".nc", "_post.nc")
    df_meta["file_path"] = destination + df_meta["file_name"]
    # rename columns
    df_meta = df_meta.rename(
        columns={
            "max_lat_N": "max_lat",
            "min_lat_S": "min_lat",
            "max_long_E": "max_lon",
            "min_long_W": "min_lon",
            "start_time": "start_datetime",
            "end_time": "end_datetime",
            "temporal_agg_type": "temporal_aggregation",
            "spatial_agg_type": "spatial_aggregation",
        }
    )
    # change literal conversion
    df_meta["temporal_resolution"] = df_meta["temporal_resolution"].map(
        {"H": "hour", "D": "day", "M": "month", "Y": "year"}
    )
    df_meta.to_csv(destination + "metadata.csv", index=False)
