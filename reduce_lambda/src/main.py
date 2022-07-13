#!/usr/bin/env python3
from functools import reduce
import json
import os
from time import time
from typing import List

import pandas as pd
import xarray as xr
import boto3

from dotenv import load_dotenv

load_dotenv()

s3 = boto3.client('s3')

s3_bucket = 'tori-calculate-wind-power'

local = False

tmp_folder = '/tmp'

input_main = 'output'
input_compressed = 'output_compressed'
output_folder = 'output_aggregated'

local_data_folder = os.path.abspath(
    os.path.join(os.path.dirname(__file__), '../../data'))

epsilon = 1e-6

emissions = 1227780.21  # kg


def get_s3_keys(prefix='') -> List[str]:
    paginator = s3.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket=s3_bucket, Prefix=prefix):
        for content in page.get('Contents', ()):
            yield content['Key']


def download_files(country_name: str, local: bool = False, compressed: bool = True) -> None:
    data_folder = local_data_folder if local else tmp_folder
    input_folder = input_compressed if compressed else input_main

    data_folder = os.path.join(
        data_folder, input_folder, country_name)
    if os.path.isdir(data_folder):
        return data_folder

    print('start download files')

    os.makedirs(data_folder)
    remote_folder = os.path.join(input_folder, country_name)
    keys = get_s3_keys(remote_folder)

    start_time = time()
    for key in keys:
        if os.path.splitext(key)[1] == '.nc4':
            file_path = os.path.join(data_folder, os.path.basename(key))
            s3.download_file(s3_bucket, key, file_path)

    print('file download time:', time() - start_time)

    return data_folder


def read_data(folder_path: str) -> pd.DataFrame:
    start_time = time()
    print('read data')

    data: List[pd.DataFrame] = []
    for file_name in os.listdir(folder_path):
        file_path = os.path.join(folder_path, file_name)
        with xr.open_mfdataset(file_path) as ds_wind:
            df = ds_wind.to_dataframe()
            data.append(df)

    df = reduce(lambda a, b: a.add(b, fill_value=0), data)

    print('time read data', time() - start_time)

    return df


def process_data(df: pd.DataFrame) -> pd.DataFrame:
    df['power_output'] = df.apply(
        lambda x: x['power_output'] + epsilon, axis=1)
    df['carbon_footprint'] = df.apply(
        lambda x: emissions / (x['power_output'] * 20 / 1000), axis=1)
    df['sed'] = df['sed'] / (365 * 24)
    df['emissions_carbon_capture'] = df.apply(
        lambda x: x['carbon_footprint'] * x['sed'], axis=1)

    return df


def save_s3(df: pd.DataFrame, country_name: str) -> None:
    output_file_path = os.path.join(tmp_folder, f'{country_name}.csv')
    df.to_csv(output_file_path)

    remote_output_file_path = os.path.join(
        output_folder, os.path.basename(output_file_path))

    s3.upload_file(output_file_path, s3_bucket, remote_output_file_path)


def lambda_handler(event, _context=None, local=False):
    country_name = event['country']
    compressed = 'compressed' in event

    folder_path = download_files(country_name, local, compressed=compressed)
    df = read_data(folder_path)
    df = process_data(df)
    print(df.head(5))
    print(df.shape)
    save_s3(df, country_name)

    return {
        'statusCode': 200,
        'body': json.dumps({
            'message': f'done processing for country {country_name}'
        })
    }


if __name__ == '__main__':
    lambda_handler({
        'country': 'Germany'
    }, local=True)
