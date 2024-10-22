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
# import xarray_regrid as xr_regrid

from utils.const import RAW_RESOLUTION

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


# def get_min_max_from_persist(pers_array):
#     v_min = pers_array.min().compute().values.item()
#     v_max = pers_array.max().compute().values.item()
#     print(f"\tMin:{v_min}, Max:{v_max}")
#     return v_min, v_max

def get_min_max_from_persist(pers_array, s=False):
    v_min = pers_array.min().compute()
    v_max = pers_array.max().compute()
    if hasattr(v_min, 'item'):
        v_min = v_min.item()
    if hasattr(v_max, 'item'):
        v_max = v_max.item()
    # print(f"\tMin:{v_min}, Max:{v_max}")
    
    if s:
        mm_str = f"{v_min},{v_max}"
        print(f'\n******************\nMM_STR TYPE: {type(mm_str)}\n******************\n')
        return mm_str
    else:
        return v_min, v_max


def get_scale_offset_from_persist(pers_array):
    v_min, v_max = get_min_max_from_persist(pers_array)
    return compute_scale_and_offset_mm(v_min, v_max)

def get_coarsen_factor(spatial_resolution: float) -> int:
    """
    Author: Yuchuan Huang
    GitHub Repo: iharp2/iharp-pure-query/src/get_data_query.py
    """
    return int(spatial_resolution / RAW_RESOLUTION)

def s_resample(xa, spatial_resolution: float, spatial_agg_method: str) -> xr.Dataset:
    """
    Author: Yuchuan Huang
    GitHub Repo: iharp2/iharp-pure-query/src/get_data_query.py

    Resample the time dimension of the given xarray.Dataset
    """
    coarsen_factor = get_coarsen_factor(spatial_resolution)
    xa = xa.coarsen(latitude=coarsen_factor, longitude=coarsen_factor, boundary="trim")
    if spatial_agg_method == "mean":
        xa = xa.mean()
    elif spatial_agg_method == "max":
        xa = xa.max()
    elif spatial_agg_method == "min":
        xa = xa.min()
    else:
        raise ValueError(f"Unknown spatial_agg_method: {spatial_agg_method}")

    return xa

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
    s_ds = xr.open_dataset(file_025, chunks='auto')

    # Get 0.25 max and min
    persisted_s = client.persist(s_ds)
    s_mm = get_min_max_from_persist(persisted_s, s=True)
    metadata.append([id_number, variable, max_lat, min_lat, max_long, min_long, start_year, end_year, temporal_aggregation, '0.25', s_mm, file_025])#s_min_str, s_max_str, file_025])
    
    # Get 0.5, 1.0 spatial aggregation 
    m_ds = s_resample(persisted_s, spatial_resolution=0.5, spatial_agg_method='mean')

    persisted_m = client.persist(m_ds)
    m_mm = get_min_max_from_persist(persisted_m, s=True)
    m_scale, m_offset = get_scale_offset_from_persist(persisted_m["t2m"])  # TODO: change this to accept any variable not just 2m temp 
    persisted_m.to_netcdf(
        file_050,
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
    metadata.append([id_number, variable, max_lat, min_lat, max_long, min_long, start_year, end_year, temporal_aggregation, '0.5', m_mm, file_050])#s_min_str, s_max_str, file_025])
    
    # 1.0 SPATIAL AGGREGATION
    l_ds = s_resample(persisted_s, spatial_resolution=1.0, spatial_agg_method='mean')

    persisted_l = client.persist(l_ds)
    l_mm = get_min_max_from_persist(persisted_l, s=True)
    l_scale, l_offset = get_scale_offset_from_persist(persisted_l["t2m"])  # TODO: change this to accept any variable not just 2m temp 
    persisted_l.to_netcdf(
        file_100,
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
    metadata.append([id_number, variable, max_lat, min_lat, max_long, min_long, start_year, end_year, temporal_aggregation, '1.0', l_mm, file_100])#s_min_str, s_max_str, file_025])
    
    return metadata