import argparse
import pandas as pd

"""
python pp2025.py -D /data/iharp-customized-storage/storage/temperature
"""
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-D", "--data_folder", type=str, help="Path to the data files folder.")
    # parser.add_argument("-M", "--metadata", type=str, help="Path to existing metadata file.")
    args = parser.parse_args()
    data_folder = args.data_folder
    # metadata = args.metadata
    metadata = data_folder + "/metadata.csv"

    print(f"Post-processing metadata: {metadata}...")
    df_meta = pd.read_csv(metadata)
    # add file_path
    df_meta["file_path"] = data_folder + "/" + df_meta["file_name"]
    # drop file_name
    df_meta = df_meta.drop(columns=["file_name"])
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
        {"H": "hour", "D": "day", "M": "month", "Y": "year", "1H": "hour"}
    )
    df_meta.to_csv(data_folder + "/0_post_metadata.csv", index=False)
    print(f"Done. New metadata {data_folder}/0_post_metadata.csv")
