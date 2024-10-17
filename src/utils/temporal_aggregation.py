'''
temporal_aggregation.py

DESCRIPTION: Contains functions used in func.py that aggregate data from
             hourly resolution to {day, month, year} resolution.

             CURRENTLY: Aggregation function code comes from the iharp-quick-aggregation GitHub repository.
             FUTURE: Aggregation function code comes from Yuchuan Huang's code for quick aggregation (prev. in iharp-quick-aggregation GitHub repo).
Author: Ana Uribe
'''

''' Imports'''
from dask.distributed import LocalCluster
import numpy as np
import xarray as xr


''' Initiate Cluster '''
cluster = LocalCluster(n_workers=10)  # Fully-featured local Dask cluster
client = cluster.get_client()

''' Helper Functions'''
def compute_scale_and_offset_mm(min, max, n=16):
    vmin = min
    vmax = max
    # stretch/compress data to the available packed range
    scale_factor = (vmax - vmin) / (2**n - 1)
    # translate the range to be symmetric about zero
    add_offset = vmin + 2 ** (n - 1) * scale_factor
    print(f"\tScale factor: {scale_factor}, add offset: {add_offset}")
    return scale_factor, add_offset


def compute_scale_and_offset(na):
    vmin = np.min(na).item()
    vmax = np.max(na).item()
    return compute_scale_and_offset_mm(vmin, vmax)


def get_min_max_from_persist(pers_array):
    v_min = pers_array.min().compute().values.item()
    v_max = pers_array.max().compute().values.item()
    print(f"\tMin:{v_min}, Max:{v_max}")
    return v_min, v_max


def get_scale_offset_from_persist(pers_array):
    v_min, v_max = get_min_max_from_persist(pers_array)
    return compute_scale_and_offset_mm(v_min, v_max)

''' Aggregation Function '''

def get_temporal_agg(file_h, file_d, file_m, file_y):
    '''
    IN: file_h (str) - path of raw (hour) .nc file to aggregate

        file_d, file_m, file_y - path to store monthly and yearly aggregations
    '''
    # DAILY AGGREGATION
    ds = xr.open_dataset(file_h, chunks={"time": 24},)
    day = ds.resample(time="D").max()

    # days.append(day)
    # day_concet = xr.concat(days, dim="time")

    persisted = client.persist(day)
    day_scale, day_offset = get_scale_offset_from_persist(persisted["t2m"])

    persisted.to_netcdf(
        file_d,
        encoding={
            "t2m": {
                "dtype": "int16",
                "missing_value": -32767,
                "_FillValue": -32767,
                "scale_factor": day_scale,
                "add_offset": day_offset,
            }
        },
    )

    # MONTHLY AGGREGATION
    month = persisted.resample(time="ME").max()
    persisted_month = client.persist(month)
    month_scale, month_offset = get_scale_offset_from_persist(persisted_month["t2m"])

    persisted_month.to_netcdf(
        file_m,
        encoding={
            "t2m": {
                "dtype": "int16",
                "missing_value": -32767,
                "_FillValue": -32767,
                "scale_factor": month_scale,
                "add_offset": month_offset,
            }
        },
    )

    # YEARLY AGGREGATION
    year = persisted_month.resample(time="YE").max()
    persisted_year = client.persist(year)
    year_scale, year_offset = get_scale_offset_from_persist(persisted_year["t2m"])

    persisted_year.to_netcdf(
        file_y,
        encoding={
            "t2m": {
                "dtype": "int16",
                "missing_value": -32767,
                "_FillValue": -32767,
                "scale_factor": year_scale,
                "add_offset": year_offset,
            }
        },
    )