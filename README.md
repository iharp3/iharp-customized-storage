# iharp-custumized-storage
Pipeline to create customized storage of iHARP data given user input.

## Introduction
It is reasonable to assume that there is a subset of the ERA5 data that is more likely to be queried by climate scientists. Scientists may want to focus on a particular time range, location, and resolution of a variable. 

Additionally, there are some queries, such as average, minimum and maximum queires, etc. commonly used by scientists that can be answered instantly from aggregations of the data.

ERA5 data cannot currently be stored locally and in general, and must be queried from the [Copernicus Data Store](https://cds.climate.copernicus.eu) in its raw format. There are some pre-processed datasets that prepare data for analysis available like [ARCO](https://console.cloud.google.com/marketplace/product/bigquery-public-data/arco-era5?pli=1&inv=1&invt=AbeUPA&project=fair-smile-414117), but the time consuming steps of downloading and aggregating the data are still necessary.

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

#### Condensing user input
Goal: determine the minimal number of API calls, download the minimal amount of data, do not download duplicates of the data.

#### Generating API calls


#### Downloading data

## Data to Customized Storage

#### Downloaded data

#### Aggregating data

#### Pruning local data

#### Final data and metadata


