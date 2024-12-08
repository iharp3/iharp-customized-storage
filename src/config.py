"""
config.py: Constants for customized storage.
"""
import os

# File paths
HOME = os.path.expanduser('~')
REPO = 'iharp-customized-storage'

DATA_D = os.path.join(HOME, REPO, 'storage')  # dir for data
USER_INTEREST = os.path.join(DATA_D, 'user_interest.csv') # file with user interest
METADATA = os.path.join(DATA_D, 'metadata.csv') # file with metadata
TO_DELETE = os.path.join(DATA_D, 'files_to_delete.csv') # file with files to delete after aggregation

# 
RAW_SP_RES = 0.25
RAW_T_RES = "H"
NUM_CHUNKS = 1000

# era5 API request settings
TIME = "valid_time"
VAR_SHORT_N = {"2m_temperature":"t2m"}
DATASET =  "reanalysis-era5-single-levels"
PRODUCT_TYPE = ["reanalysis"]
DATA_FORMAT = "netcdf"
DOWNLOAD_FORMAT = "unarchived"
MONTH_RANGE = [
        "01", "02", "03",
        "04", "05", "06",
        "07", "08", "09",
        "10", "11", "12"
    ]
DAY_RANGE = [
        "01", "02", "03",
        "04", "05", "06",
        "07", "08", "09",
        "10", "11", "12",
        "13", "14", "15",
        "16", "17", "18",
        "19", "20", "21",
        "22", "23", "24",
        "25", "26", "27",
        "28", "29", "30",
        "31"
    ]
TIME_RANGE = [
        "00:00", "01:00", "02:00",
        "03:00", "04:00", "05:00",
        "06:00", "07:00", "08:00",
        "09:00", "10:00", "11:00",
        "12:00", "13:00", "14:00",
        "15:00", "16:00", "17:00",
        "18:00", "19:00", "20:00",
        "21:00", "22:00", "23:00"
    ]
# MONTH_RANGE =  [str(i).zfill(2) for i in range(1, 13)],
# DAY_RANGE = [str(i).zfill(2) for i in range(1, 32)],
# TIME_RANGE = [f"{str(i).zfill(2)}:00" for i in range(0, 24)]