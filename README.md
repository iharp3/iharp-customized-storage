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

## User Input to Customized Storage

#### Instructions:
To get from the user input to the downloaded data, these are the steps to follow:

1. In `const.py`: update the `u_in_file_name` variable to the file name of the user input you want.
2. In `const.py`: if you are on cluster 501, update `cluster_501` to `True`. If you are on another machine, make sure the file paths are the ones you want.
2. Run `v0_customized_storage.py`

#### Getting user input

**Currently:** 
We take a user input to be a CSV with information regarding the time and spatial resoultion, location (in latitude and longitude degrees), and time range needed for a variable. While day, time, and month can be specified for the time range, we only show the year below in the `start_time` and `end_time` columns because despite user input, we will download the whole year of any year that is at least partly selected in order to be able to aggregate that time range.

Example user input currently in `user_input.csv`:

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

#### Condensing user input
**Currently:** 
For the initial version of the pipeline, we will create an API call for every row. This will result in duplicate data, which we will handle in the future.

**Future Plans:**
Once user input is given, we need to figure out the necessary subset of data that satisfies all the user needs. Our goal in this is to download the minimal amount of data needed.

Since we will query the data at the highest temporal and spatial resolution from CDS (hourly and 0.25 degrees respectively), we only need to figure out if, for any variable, there is any overlap in the *location* and *time range*.

If there is, we can merge the row requests, and later on, delete the extra data we do not need if there is data that is just needed at a coarser resolution. If there is not any overlap in the user input, each row will correspond to an API call to CDS.

The overlap needs to be strictly for both location and time range. If only one of these dimensions overlap, creating calls for each one individually downloads less data overall and thus is more efficient.

#### Generating API calls and downloading data
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
                    'variable': [row['variable']],
                    'year': [2003],
                    'month': api_request_settings['month'],
                    'day': api_request_settings['day'],
                    'time': api_request_settings['time'],
                    'data_format': api_request_settings['data_format'],
                    'download_format': api_request_settings['download_format'],
                    'area': [row['max_lat'], row['min_long'], row['min_lat'], row['max_long']] Order: [N, W, S, E]
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

This downloads all the data asked for in the user input to our local storage in an hourly temporal resolution and a 0.25 degree spatial resolution. Now it needs to be processed. Each API call downloads one file unless the data requested is too large. For now, we assume that each call produces one file, so each row in the user input has the corresponding data in the given file name.

#### Creating customized storage
When all the data is downloaded, we need to aggregate and prune it to create our customized storage. Below is a table of the data we compute for each downloaded file.

|                   |  Computed Resolutions (Aggregations) |                                   Statistics Calculated                                  |
|:-----------------:|:------------------------------------:|:----------------------------------------------------------------------------------------:|
|      Temporal     | Hourly (raw), Daily, Monthly, Yearly | minimum, maximum, mean at each resolution, and the minimum and maximum of the whole file |
| Spatial (degrees) |         0.25 (raw), 0.5, 1.0         | minimum, maximum, mean at each resolution, and the minimum and maximum of the whole file |

For every desired subset of the data, we keep all coarser resolutions and prune the finer resolutions. For example, if a region is needed at a monthly temporal resolution, we delete the hourly and daily temporal aggregations and keep the monthly and yearly ones. If a subset of the data is needed at the 0.5 degree resolution, we delete the 0.25 degree resolution and keep the 1.0 degree aggregation.

Thus, the highest number of files we can have for a certain region is when we need the data at an hourly and 0.25 resolution, so we must keep:
* Hourly data at a 0.25, 0.5, and 1.0 degree resolution,
* Daily data at a 0.25, 0.5, and 1.0 degree resolution,
* Monthly data at a 0.25, 0.5, and 1.0 degree resolution, and
* Yearly data at a 0.25, 0.5, and 1.0 degree resolution.
which is a total of 12 files.

The minimum number of files we can have for a certain region is when we need the data at a yearly and 1.0 resolution, so we only need to keep this one file.

#### Aggregating temporal data
We first aggregate all the data temporally, calculating the daily minimum, maximum, and mean from the raw (hourly) data, then the monthly minimum, maximum, and mean aggregation from the daily resolution, and finally the same statistics for the yearly resolution.

**Currently:**
Each aggregation is saved in a separate file with the following naming convention:

`agg_<file path>_<temporal resolution>.nc`

where `file path` is determined by the file path name assigned to the raw data in the user input, and `temporal resolution` is either `day, month`, or `year`. Note the original file contains the hourly and 0.25 resolution data.

#### Pruning local data

Once we have all the data at all the temporal resolutions, we can prune what we don't need.

**Currently:**
We determine what we do not need using the user input file, where the desired spatial and temporal resolutions for each row are specified. We take this information and determine the names of the aggregated files that can be deleted. Below is an example of the files that can be deleted for the first row in the example user input above.

| id | file_names                                           |
|----|------------------------------------------------------|
| 1  |   raw_1.nc,   agg_1_day.nc,   agg_1_month.nc    |
<!-- | 2  |   raw_2.nc                                      |
| 3  |                                                      |
| 4  |   raw_4.nc,   agg_4_day.nc                      |
| 5  |   raw_5.nc,   agg_5_day.nc,   agg_5_month.nc    |
| 6  |   raw_6.nc,   agg_6_day.nc                      |
| 7  |   raw_7.nc                                      |
| 8  |                                                      |
| 9  |                                                      |
| 10 |                                                      |
 -->

Since row 1 asks for a yearly resolution, so we delete all the coarser resolutions.

#### Aggregating spatial data
Once we have pruned the data in terms of the temporal aggregation, we  calculate the coarser spatial resolutions of the remaining data. This is often called "re-gridding". To increase scan speed, we save the maximum and minimum values of each file.

**Currently:**
Like the temporal resolutions, each aggregation is saved in a separate file with the following naming convention:

`agg_<file path>_<temporal resolution>_<spatial resolution>.nc`

where `file path` is determined by the file assigned to the raw data, `temporal resolution` is the temporal resolution of the file, and `spatial resolution` is either 0.5 or 1.0 degrees.

#### Pruning local data (part 2)

Once we have all the temporaly aggregated data at all the spatial resolutions, we can prune what we don't need.

**Currently:**
We determine what we do not need using the user input file, where the desired spatial and temporal resolutions for each row are specified. We take this information and determine the names of the aggregated files that can be deleted. Below is an example of the files that can be deleted for the first row in the example user input above.

| id | file_names                         |
|----|------------------------------------|
| 1  |  agg_1_\*\_025.nc,  agg_1_*_050.nc   |
<!-- | 2  |  agg_2_*_025.nc                    |
| 3  |                                    |
| 4  |  agg_4_*_025.nc                    |
| 5  |                                    |
| 6  |  agg_6_\*\_025.nc,  agg_6_*_050.nc   |
| 7  |  agg_7_*_025.nc                    |
| 8  |                                    |
| 9  |                                    |
| 10 |                                    |-->

Since the first row asks for 1.0 degree resolutions, the 0.25 and 0.5 degree files can be delted. The `*` signify that all temporal resolutions that have this spatial resolution should be deleted.

#### Final data and metadata
Now we have a storage customized for the user interest.

We also have the following metadata for each file:
* All the information from the user input row: variable, location range, time range.
* File path
* File minimum and maximum values

For example, the output metadata for the first and second rows of the user input is shown below. 

For row 1, which asked for yearly and 1.0 degree resolution, we only have one file where the data is aggregated to the coarsest values.

For row 2, which asked for daily and 0.5 degree resolution, we have
* Daily, monthly, and yearly resolutions at a 0.5 degree spatial resolution
* Daily, monthly, and yearly resolutions at a 1.0 degree spatial resolution

| id_number | variable       | max_lat | min_lat | max_long | min_long | start_time | end_time | time_resolution | spatial_resolution | file_min | file_max | file_path                       |
|-----------|----------------|---------|---------|----------|----------|------------|----------|-----------------|--------------------|----------|----------|---------------------------------|
| 1         | 2m_temperature | -30     | -40     | 40       | 30       | 2003       | 2003     | year            | 1.0                | 295.075  | 307.309  | /../data/agg_1_year_100.nc  |
| 2         | 2m_temperature | 10      | 0       | 0        | -20      | 2021       | 2023     | day             | 0.5                | 293.100  | 314.846  | /../data/agg_2_day_050.nc   |
| 2         | 2m_temperature | 10      | 0       | 0        | -20      | 2021       | 2023     | day             | 1.0                | 294.031  | 313.989  | /../data/agg_2_day_100.nc   |
| 2         | 2m_temperature | 10      | 0       | 0        | -20      | 2021       | 2023     | month           | 0.5                | 296.950  | 314.846  | /../data/agg_2_month_050.nc |
| 2         | 2m_temperature | 10      | 0       | 0        | -20      | 2021       | 2023     | month           | 1.0                | 297.124  | 313.989  | /../data/agg_2_month_100.nc |
| 2         | 2m_temperature | 10      | 0       | 0        | -20      | 2021       | 2023     | year            | 0.5                | 301.302  | 314.846  | /../data/agg_2_year_050.nc  |
| 2         | 2m_temperature | 10      | 0       | 0        | -20      | 2021       | 2023     | year            | 1.0                | 301.384  | 314.112  | /../data/agg_2_year_100.nc  |
Thus, we can say that the number of files we get from one user-input row is the number of temporal resolutions we want to keep times the number of spatial resolutions we want to keep.

**Future:**
In the future, each "user-input" row might be a fraction of a user-input row, or a combination of several user-input rows. This is to minimize the amount of duplicate data in storage.
