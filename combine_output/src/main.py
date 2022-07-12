#!/usr/bin/env python3
import os
from time import time
from typing import List

import pandas as pd
import xarray as xr
import boto3

from dotenv import load_dotenv
from tqdm import tqdm

load_dotenv()

s3 = boto3.client('s3')

s3_bucket = 'tori-calculate-wind-power'

input_folder = 'output_aggregated'
data_folder = os.path.join(os.path.abspath(__file__), '../../data')

epsilon = 1e-6

emissions = 1227780.21  # kg


def get_s3_keys(prefix='') -> List[str]:
    paginator = s3.get_paginator('list_objects_v2')
    for page in tqdm(paginator.paginate(Bucket=s3_bucket, Prefix=prefix)):
        for content in page.get('Contents', ()):
            yield content['Key']


def download_files() -> None:
    output_folder = os.path.join(
        data_folder, input_folder)
    os.makedirs(output_folder)
    keys = get_s3_keys(input_folder)
    for key in keys:
        if os.path.splitext(key)[1] == '.nc4':
            s3.download_file(s3_bucket, key, output_folder)


def read_data() -> pd.DataFrame:
    files_in_folder = os.path.join(
        os.path.join(data_folder, input_folder), '*.nc4')

    start_time = time()
    print('read data')
    with xr.open_mfdataset(files_in_folder, combine='by_coords') as ds_wind:
        df = ds_wind.to_dataframe()
    print('time read data', time() - start_time)

    return df


def process_data(df: pd.DataFrame) -> pd.DataFrame:
    coordinates = [point['coordinates'] for point in df['location'].values]
    coordinates = [(coord[1], coord[0]) for coord in coordinates]
    index = pd.MultiIndex.from_tuples(coordinates, names=['lat', 'lon'])
    df.set_index(index, inplace=True)
    df.drop(['location'], axis=1, inplace=True)

    df['power_output'] = df.apply(
        lambda x: x['power_output'] + epsilon, axis=1)
    df['carbon_footprint'] = df.apply(
        lambda x: emissions / (x['power_output'] * 20 / 1000), axis=1)
    df['sed'] = df['sed'] / (365 * 24)
    df['emissions_carbon_capture'] = df.apply(
        lambda x: x['carbon_footprint'] * x['sed'], axis=1)

    return df


def save_s3(df: pd.DataFrame) -> None:
    output_file_path = os.path.join('/tmp', 'all_data.csv')
    df.to_csv(output_file_path)

    remote_output_file_path = os.path.join(
        input_folder, os.path.basename(output_file_path))

    s3.upload_file(output_file_path, s3_bucket, remote_output_file_path)


def main() -> None:
    download_files()
    df = read_data()
    df = process_data(df)
    print(df.head(5))
    print(df.shape)
    save_s3(df)


if __name__ == '__main__':
    main()
