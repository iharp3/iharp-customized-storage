'''
customized_storage.py: Main function that creates customized storage.
'''
import os
import time

from config import USER_INTEREST
from utils import load_csv
from ApiGenerator import API_Call

# Upload user input
user_interest_rows = load_csv(USER_INTEREST)

for row in user_interest_rows:
    # Create file name
    raw_file_name = get_raw_file_name(row['variable'])

    # Create API call
    call = API_Call(row, raw_file_name)
    call.era5_api_request()
    call.make_file()

    if wait_for_file(raw_file_name):
        print(f"\tData downloaded to: {raw_file_name}.")
    else:
        user_interest_rows.append(row)

    # Temporally aggregate file (return metadata with list of file names to aggregate spatially, and list of files to delete)
        # Spatially aggregate file (return metadata and list of files to delete)

# Save metadata
# Check before deleting files to delete


# turn start and end time into datetime objects
# user_input_df[col_name['start_t']] = pd.to_datetime(user_input_df[col_name['start_t']], format='%d-%m-%Y')
# user_input_df[col_name['end_t']] = pd.to_datetime(user_input_df[col_name['end_t']], format='%d-%m-%Y')

'''Get necessary API calls and keep track of resolutions needed'''
# TODO: use groupby and apply for this

