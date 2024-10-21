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
    if res == 0.5 or res == 1.0:
        grid = xarray_regrid.Grid(
            north=n,
            south=s,
            west=w,
            east=e,
            resolution_lat=res,
            resolution_lon=res,
        ).create_regridding_dataset()

        return grid
    
    else:
        print(f'\tSpatial resolution {res} not supported. Supported vals = 0.5, 1.0')

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
    day_scale, day_offset = get_scale_offset_from_persist(persisted["t2m"]) # TODO: change this to accept any variable not just 2m temp

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
    month_scale, month_offset = get_scale_offset_from_persist(persisted_month["t2m"]) # TODO: change this to accept any variable not just 2m temp

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
    year_scale, year_offset = get_scale_offset_from_persist(persisted_year["t2m"]) # TODO: change this to accept any variable not just 2m temp

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

def get_spatial_agg(file_025, file_050, file_100, id_number, temporal_aggregation, client, variable, max_lat, min_lat, max_long, min_long, start_year, end_year):
    '''
    IN: file_025 (str) - path of raw (0.25 degree) .nc file to aggregate

        file_050, file_100 (str) - paths to put agg files

        id_number, temporal_aggregation (str) - current file_025's id number and temporal agg to make file name and metadata entry

        client (Dask client object) - local cluster object to speed up aggregation

        variable, max_... (str) - info about file

    OUT: metadata (list) - list with <id_number, variable, max_lat, min_lat, max_long, min_long, start_year, end_year,
                                             temporal_aggregation, spatial_aggregation, file_min_value, file_max_value>
                          for each file created 
    '''
    metadata = []

    ds = xr.open_dataset(file_025)
    renamed_ds = ds.rename({'valid_time':'time'})
    
    # 0.25 SPATIAL AGGREGATION
    persisted_s = client.persist(renamed_ds)
    s_min, s_max = get_min_max_from_persist(persisted_s)
    metadata.append([id_number, variable, max_lat, min_lat, max_long, min_long, start_year, end_year, temporal_aggregation, '0.25', s_min, s_max, file_025])


    # 0.50 SPATIAL AGGREGATION
    grid_m = get_grid(n=max_lat, s=min_lat, w=min_long, e=max_long, res=0.5)
    m_ds = persisted_s.regrid.stat(grid_m, method='mean', time_dim='time', skipna=False)

    persisted_m = client.persist(m_ds)
    m_min, m_max = get_min_max_from_persist(persisted_m)
    m_scale, m_offset = get_scale_offset_from_persist(persisted_m["t2m"])   # TODO: change this to accept any variable not just 2m temp
    
    persisted_m.to_netcdf(
        file_025,
        encoding={
            "t2m": {
                    "dtype": "int16",
                    "missing_value": -32767,
                    "_FillValue": -32767,
                    "scale_factor": m_scale,
                    "add_offset": m_offset,
            }
        }
    )
    metadata.append([id_number, variable, max_lat, min_lat, max_long, min_long, start_year, end_year, temporal_aggregation, '0.5', m_min, m_max, file_050])
    
    # 1.0 SPATIAL AGGREGATION
    grid_l = get_grid(n=max_lat, s=min_lat, w=min_long, e=max_long, res=1.0)
    l_ds = persisted_m.regrid.stat(grid_l, method='mean', time_dim='time', skipna=False)

    persisted_l = client.persist(l_ds)
    l_min, l_max = get_min_max_from_persist(persisted_l)
    l_scale, l_offset = get_scale_offset_from_persist(persisted_l["t2m"])   # TODO: change this to accept any variable not just 2m temp
    
    persisted_l.to_netcdf(
        file_025,
        encoding={
            "t2m": {
                    "dtype": "int16",
                    "missing_value": -32767,
                    "_FillValue": -32767,
                    "scale_factor": l_scale,
                    "add_offset": l_offset,
            }
        }
    )
    metadata.append([id_number, variable, max_lat, min_lat, max_long, min_long, start_year, end_year, temporal_aggregation, '1.0', l_min, l_max, file_100])

    return metadata