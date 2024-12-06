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