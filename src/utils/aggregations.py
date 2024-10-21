'''
temporal_aggregation.py

DESCRIPTION: Contains functions used in func.py that aggregate data from
             hourly resolution to {day, month, year} resolution.

             CURRENTLY: Aggregation function code comes from the iharp-quick-aggregation GitHub repository.
             FUTURE: Aggregation function code comes from Yuchuan Huang's code for quick aggregation (prev. in iharp-quick-aggregation GitHub repo).
Author: Ana Uribe
'''

''' Imports'''
# from dask.distributed import LocalCluster
import numpy as np
import xarray as xr
import xarray_regrid

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

def get_grid(n, s, w, e, res):
    '''
    IN: n, s, w, e (float) - boundaries of the grid

        res (float) - lat/long resolution (Supported: 0.5, 1.0)

    OUT: grid (Grid obj) - as defined with inputs
    '''
    grid = xarray_regrid.Grid(
        north=n,
        south=s,
        west=w,
        east=e,
        resolution_lat=res,
        resolution_lon=res,
    ).create_regridding_dataset()

    return grid

''' Aggregation Functions '''

def get_temporal_agg(file_h, file_d, file_m, file_y, client):
    '''
    IN: file_h (str) - path of raw (hour) .nc file to aggregate

        file_d, file_m, file_y (str) - path to store monthly and yearly aggregations

        client (Dask client object) - local cluster object to speed up aggregation
    '''
    # DAILY AGGREGATION
    ds = xr.open_dataset(file_h, chunks={"valid_time": 24},)
    renamed_ds = ds.rename({'valid_time':'time'})
    day = renamed_ds.resample(time="D").max()

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

def get_spatial_agg(file_025, file_050, file_100, id_number, temporal_aggregation, client):
    '''
    IN: file_025 (str) - path of raw (0.25 degree) .nc file to aggregate

        file_050, file_100 (str) - paths to put agg files

        id_number, temporal_aggregation (str) - current file_025's id number and temporal agg to make file name and metadata entry

        client (Dask client object) - local cluster object to speed up aggregation

    OUT: metadata (list) - list with <id_number, temporal_aggregation, spatial_aggregation, file_min_value, file_max_value>
                          for each file created 
    '''
    metadata = []

    ds = xr.open_dataset(file_025)
    renamed_ds = ds.rename({'valid_time':'time'})
    

    metadata.append([id_number, temporal_aggregation, '0.5', f_min, f_max, file_050 ])
    metadata.append([id_number, temporal_aggregation, '1.0', f_min, f_max, file_100 ])