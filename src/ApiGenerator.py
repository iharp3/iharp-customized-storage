"""
ApiGenerator.py: Creates and manages API calls to remote data repositories.
"""
import numpy as np

from utils import get_data_path

class API_Call:
    def __init__(self, row, name):
        self.row = row  # user input row
        self.name = name    # file name

    def get_year_range(self):
        start_year = (self.row['start_time']).astype('M8[ns]').item().year
        end_year = (self.row['end_time']).astype('M8[ns]').item().year

        return [str(i) for i in range(int(start_year), int(end_year) + 1)]

    def get_area_list(self):
        n = self.row['max_lat_N']
        w = self.row['min_long_W']
        s = self.row['min_lat_S']
        e = self.row['max_long_E']

        return [n, w, s, e]

    def era5_api_request(self):

        request = {"product_type": config.PRODUCT_TYPE,
                   "variable": self.row['variable'],
                   "year": self.get_year_range(),
                   "month": config.MONTH_RANGE,
                   "day": config.DAY_RANGE,
                   "time": config.TIME_RANGE,
                   "data_format": config.DATA_FORMAT,
                   "download_format": config.DOWNLOAD_FORMAT,
                   "area": get_area_list()
        }

        return request

    def make_file(self):
        file_path = get_data_path(self.name)

        dataset = config.DATASET
        request = era5_api_request()

        client = cdsapi.Client()

        print(f"\tDownloading {self.row['variable']} data to {file_path}.")
        client.retrieve(dataset,request,file_path)