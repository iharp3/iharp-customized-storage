# iharp-customized-storage
Pipeline to create customized storage of iHARP data given user input.

## Introduction
It is reasonable to assume that there is a subset of the [ERA5](https://www.ecmwf.int/en/forecasts/dataset/ecmwf-reanalysis-v5) data that is more likely to be queried by climate scientists. Scientists may want to focus on a particular time range, location, and resolution of a variable. 

Additionally, there are some queries, such as average, minimum and maximum queires, etc. commonly used by scientists that can be answered instantly from aggregations of the data.

Due to its size, the ERA5 data cannot currently be stored locally and in general, and must be queried from the [Copernicus Data Store](https://cds.climate.copernicus.eu)(CDS) in its raw format. There are some pre-processed datasets that prepare data for analysis available like [ARCO](https://console.cloud.google.com/marketplace/product/bigquery-public-data/arco-era5?pli=1&inv=1&invt=AbeUPA&project=fair-smile-414117), but the time consuming steps of downloading and aggregating the data are still necessary.

Our goal is to locally store and pre-aggregate a subset of the ERA5 data such that the answer to 80 percent of scientist queires are pre-computed, and thus available instantly without each scientist having to download data, pre-process it, and create code to answer the query themselves.

## Overview

To achieve our goal, we need the following:
* User input: in order for the locally stored data (and its aggregation) to be able to answer most queires without needing additional data from Copernicus, the locally stored data must reflect what the scientists, the users, are interested in.

* Data aggregation: many queires require temporal or spatial aggregation of the data, which can be done beforehand.

* Query API or package: for users to query the customized storage, an API or python package is needed that can take the user query and return the answer from the local storage, or in the worst case, download more data from Copernicus to compute the answer.

This repository focuses on:
- [User Input to Data](#user-input-to-data): Taking in the user input and determining the necessary data that needs to be downloaded.
- [Data to Customized Storage](#data-to-customized-storage): Creating the customized storage with aggregated data.

## User Input to Data

### Instructions:
To get from the user input to the downloaded data, these are the steps to follow:

1. In `const.py`: update `V_ZERO_USER_INPUT_FILE_PATH` variable to the file path of the user input.
2. Run `v0_customized_storage.py`

#### Getting user input

**Currently:** 
We take a user input to be a CSV with information regarding the time and spatial resoultion, location (in latitude and longitude degrees), and time range needed for a variable. While day, time, and month can be specified for the time range, we only show the year below in the `start_time` and `end_time` columns because despite user input, we will download the whole year of any year that is at least partly selected in order to be able to aggregate that time range.

Example user input:

|         variable        | max_lat | min_lat | max_long | min_long | start_time | end_time | temporal_resolution | spatial_resolution | file_path |
|:-----------------------:|:-------:|:-------:|:--------:|:--------:|:----------:|:--------:|:-------------------:|:------------------:|:---------:|
|      2m_temperature     |   -30   |   -40   |    40    |    30    |    2003    |   2003   |         year        |          1         |  raw_1.nc |
|      2m_temperature     |    10   |    0    |     0    |    -20   |    2021    |   2023   |         day         |         0.5        |  raw_2.nc |
|      2m_temperature     |    30   |    10   |    -30   |    -40   |    2016    |   2018   |         hour        |        0.25        |  raw_3.nc |
|      2m_temperature     |    10   |   -10   |    -30   |    -40   |    2007    |   2009   |        month        |         0.5        |  raw_4.nc |
|      2m_temperature     |    40   |    20   |    10    |    -10   |    2009    |   2011   |         year        |        0.25        |  raw_5.nc |
|      2m_temperature     |    10   |    0    |    20    |     0    |    2020    |   2021   |        month        |          1         |  raw_6.nc |
|      2m_temperature     |    40   |    10   |    40    |    20    |    2023    |   2023   |         day         |         0.5        |  raw_7.nc |
|      2m_temperature     |    20   |    0    |    10    |    -10   |    2020    |   2022   |         hour        |        0.25        |  raw_8.nc |
|      2m_temperature     |    40   |    10   |    -10   |    -40   |    2017    |   2020   |         hour        |        0.25        |  raw_9.nc |
|      2m_temperature     |    40   |    20   |    30    |     0    |    2008    |   2010   |         hour        |        0.25        | raw_10.nc |
| 10m_u_component_of_wind |    0    |   -30   |    -10   |    -40   |    2013    |   2016   |         year        |          1         | raw_11.nc |
| 10m_u_component_of_wind |    0    |   -30   |    30    |    -10   |    2013    |   2016   |        month        |         0.5        | raw_12.nc |
| 10m_u_component_of_wind |    30   |    0    |    30    |    -10   |    2013    |   2016   |         day         |         0.5        | raw_13.nc |
| 10m_u_component_of_wind |    30   |    0    |    -10   |    -40   |    2013    |   2016   |         hour        |        0.25        | raw_14.nc |
| 10m_v_component_of_wind |    0    |   -30   |    -10   |    -40   |    2013    |   2016   |         year        |          1         | raw_15.nc |
| 10m_v_component_of_wind |    0    |   -30   |    30    |    -10   |    2013    |   2016   |        month        |         0.5        | raw_16.nc |
| 10m_v_component_of_wind |    30   |    0    |    30    |    -10   |    2013    |   2016   |         day         |         0.5        | raw_17.nc |
| 10m_v_component_of_wind |    30   |    0    |    -10   |    -40   |    2013    |   2016   |         hour        |        0.25        | raw_18.nc |

#### Condensing user input
**Currently:** 
For the initial version of the pipeline, we will create an API call for every row. This will result in duplicate data, which we will handle in the future.

**Future Plans:**
Once user input is given, we need to figure out the necessary subset of data that satisfies all the user needs. Our goal in this is to download the minimal amount of data needed.

Since we will query the data at the highest temporal and spatial resolution from CDS (hourly and 0.25 degrees respectively), we only need to figure out if, for any variable, there is any overlap in the *location* and *time range*.

If there is, we can merge the row requests, and later on, delete the extra data we do not need if there is data that is just needed at a coarser resolution. If there is not any overlap in the user input, each row will correspond to an API call to CDS.

The overlap needs to be strictly for both location and time range. If only one of these dimensions overlap, creating calls for each one individually downloads less data overall and thus is more efficient.

#### Generating API calls
**Current:** 
For each row of the user input, we generate one API call. 

For the following row: 
|         variable        | max_lat | min_lat | max_long | min_long | start_time | end_time | temporal_resolution | spatial_resolution | file_path |
|:-----------------------:|:-------:|:-------:|:--------:|:--------:|:----------:|:--------:|:-------------------:|:------------------:|:---------:|
|      2m_temperature     |   -30   |   -40   |    40    |    30    |    2003    |   2003   |         year        |          1         |  raw_1.nc |

We have the following API call:

    dataset = api_request_settings['dataset']
                request = {
                    'product_type': api_request_settings['product_type'],
                    'variable': [row['variable'],
                    'year': [2003],
                    'month': api_request_settings['month'],
                    'day': api_request_settings['day'],
                    'time': api_request_settings['time'],
                    'data_format': api_request_settings['data_format'],
                    'download_format': api_request_settings['download_format'],
                    'area': [row['max_lat'], row['min_long'], row['min_lat'], row['max_long']]  # Order: [N, W, S, E]
                }
                
                client = cdsapi.Client()
                client.retrieve(dataset, request).download(file_path)

where `row` is the example row, so the `row['variable']` corresponds to the value in the 'variable' column of the row, etc., and the `api_request_settings` dictionary is the following:

      api_request_settings = {
      "dataset": "reanalysis-era5-single-levels",
      "product_type": ["reanalysis"],
      "data_format": "netcdf",
      "download_format": "unarchived",
      "month": [str(i).zfill(2) for i in range(1, 13)],
      "day": [str(i).zfill(2) for i in range(1, 32)],
      "time":[f"{str(i).zfill(2)}:00" for i in range(0, 24)]
      }
#### Downloading data
**Currently:**
We download the data to our local storage.

## Data to Customized Storage

#### Downloaded data

#### Aggregating data

Temporal Aggregation:


Spatial Aggregation:


#### Pruning local data

#### Final data and metadata


