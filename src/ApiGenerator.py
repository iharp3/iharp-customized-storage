"""
ApiGenerator.py: Creates and manages API calls to remote data repositories.
"""

# Need to use YYY-MM-DD time

class API_Call:
    # self  - all diff row values
    #       - repository

    # def   - era5_api_request()
    #       - make_file()
    #
    def __init__(self, row, name):
        self.row = row  # user interest row
        self.name = name    # file name

    def era5_api_request(self):

    def make_file(self):