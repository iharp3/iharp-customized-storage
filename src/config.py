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
TIME = "valid_time"
RAW_SP_RES = 0.25
RAW_T_RES = "H"
NUM_CHUNKS = 1000

# era5 API request settings
DATASET =  "reanalysis-era5-single-levels"
PRODUCT_TYPE = ["reanalysis"]
DATA_FORMAT = "netcdf"
DOWNLOAD_FORMAT = "unarchived"
MONTH_RANGE =  [str(i).zfill(2) for i in range(1, 13)],
DAY_RANGE = [str(i).zfill(2) for i in range(1, 32)],
TIME_RANGE = [f"{str(i).zfill(2)}:00" for i in range(0, 24)]