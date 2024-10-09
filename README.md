# iharp-custumized-storage
Pipeline to create customized storage of iHARP data given user input.

## Introduction
It is reasonable to assume that there is a subset of the ERA5 data that is more likely to be queried by climate scientists. Scientists may want to focus on a particular time range, location, and resolution of a variable. 

Additionally, there are some queries, such as average, minimum and maximum queires, etc. commonly used by scientists that can be answered instantly from aggregations of the data.

ERA5 data cannot currently be stored locally and in general, and must be queried from the [Copernicus Data Store](https://cds.climate.copernicus.eu)(CDS) in its raw format. There are some pre-processed datasets that prepare data for analysis available like [ARCO](https://console.cloud.google.com/marketplace/product/bigquery-public-data/arco-era5?pli=1&inv=1&invt=AbeUPA&project=fair-smile-414117), but the time consuming steps of downloading and aggregating the data are still necessary.

Our goal is to locally store and pre-aggregate a subset of the ERA5 data such that the answer to 80 percent of scientist queires are pre-computed, and thus available instantly without each scientist having to download data, pre-process it, and create code to answer the query themselves.

## Overview

To achieve our goal, we need the following:
* User input: in order for the locally stored data (and its aggregation) to be able to answer most queires without needing additional data from Copernicus, the locally stored data must reflect what the scientists, the users, are interested in.

* Data aggregation: many queires require temporal or spatial aggregation of the data, which can be done beforehand.

* Query API or package: for users to query the customized storage, an API or python package is needed that can take the user query and return the answer from the local storage, or in the worst case, download more data from Copernicus to compute the answer.

This repository focuses on:
- [User Input to Data](#user-input-to-data): Taking in the user input and determining the necessary data downloading and aggregation that needs to occur
- [Data to Customized Storage](#data-to-customized-storage): Creating the customized storage.

## User Input to Data

### Instructions:
To get from the user input to the downloaded data, these are the steps to follow:

1.

#### Getting user input

Currently, we take a user input to be a CSV with information regarding the time and spatial resoultion, location (in latitude and longitude degrees), and time range needed for a variable.

| **row** |   **variable**   | **time resolution** | **spatial resolution** | **max_latitude** | **min_latitude** | **max_longitude** | **min_longitude** | **start_time** | **end_time** |      **relation**      |
|:-------:|:----------------:|:-------------------:|:----------------------:|:----------------:|:----------------:|:-----------------:|:-----------------:|:--------------:|:------------:|:----------------------:|
|    1    |    temperature   |         hour        |          0.25          |        30        |        20        |         30        |         20        |    1-1-2020    |   1-1-2023   |                        |
|    2    |    temperature   |         hour        |           0.5          |        20        |        10        |         20        |         10        |    1-1-1980    |   1-1-2020   |                        |
|    3    |    temperature   |         day         |          0.25          |        20        |        10        |         10        |         0         |    6-1-2000    |   6-1-2020   |                        |
|    4    |    temperature   |        month        |           0.5          |        20        |        10        |         30        |         20        |    1-1-1970    |   1-1-2024   |                        |
|    5    |    temperature   |         year        |          0.25          |        10        |         0        |         10        |         0         |    1-1-2000    |   1-1-2020   |                        |
|    6    |    temperature   |        month        |           0.5          |        20        |        10        |         20        |         10        |    1-1-1980    |   1-1-2020   |     agg. of row 3      |
|    7    |    temperature   |         hour        |          0.25          |        25        |        20        |         25        |         20        |    1-1-2020    |  31-12-2020  |     subset of row 1    |
|    8    |    temperature   |         year        |            1           |        10        |        -10       |         10        |        -10        |    1-1-1970    |   1-1-2024   |  agg. of part of row 5 |
|    9    |    temperature   |         day         |           0.5          |        20        |        10        |         25        |         20        |    1-1-2020    |   1-1-2023   |  agg. of part of row 8 |
|    10   |    temperature   |         year        |           0.5          |        30        |         0        |         30        |         0         |     1-1970     |   1-1-2024   |  agg. of part of row 1 |
|    12   | u-component wind |         hour        |          0.25          |        30        |        20        |         30        |         20        |    1-1-2020    |   1-1-2023   |     row 1 for wind     |
|    13   | v-component wind |         hour        |          0.25          |        30        |        20        |         30        |         20        |    1-1-2020    |   1-1-2023   |     row 1 for wind     |
|    14   | u-component wind |        month        |           0.5          |        20        |        10        |         20        |         10        |    1-1-2000    |   1-1-2020   | part of row 6 for wind |
|    15   | v-component wind |        month        |           0.5          |        20        |        10        |         20        |         10        |    1-1-2000    |   1-1-2020   | part of row 6 for wind |

#### Condensing user input
Once user input is given, we need to figure out the necessary subset of data that satisfies all the user needs. 

Since we will query the data at the highest temporal and spatial resolution from CDS (hourly and 0.25 degrees respectively), we only need to figure out if, for any variable, there is any overlap in the location and time range.

Goal: determine the minimal number of API calls, download the minimal amount of data, do not download duplicates of the data.



#### Generating API calls


#### Downloading data

## Data to Customized Storage

#### Downloaded data

#### Aggregating data

#### Pruning local data

#### Final data and metadata


