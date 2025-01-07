# Calculate size of all files (in MB)
import pandas as pd

metadata_filename = '/data/iharp-customized-storage/storage/post/metadata.csv'

metadata = pd.read_csv(metadata_filename)

total_storage = sum(metadata['file_size'])
print(total_storage)