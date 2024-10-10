'''
func.py

DESCRIPTION: Contains functions that are used in customized_storage.py

Author: Ana Uribe
'''

'''Imports'''
import pandas as pd

from const import col_name



'''
FUNC: get_time_range_ids

IN: df (pandas data frame) - rows with start and end datetimes
OUT: df (pandas data frame) - df with new range_id col
'''
def get_time_range_ids(df):

    s = col_name["start_t"]
    e = col_name["end_t"]

    df = df.sort_values(by=s).reset_index(drop=True)
    cur_end = df.iloc[0][e]
    cur_range_id = 1
    df["range_id"] = cur_range_id

    for i in range(1, len(df)):
        next_start = df.iloc[i][s]
        next_end = df.iloc[i][e]

        if next_start <= cur_end:
            cur_end = max(cur_end, next_end)
            df.loc[i, "range_id"] = cur_range_id
        else:
            cur_range_id += 1
            cur_end = next_end
            df.loc[i, "range_id"] = cur_range_id

    return df