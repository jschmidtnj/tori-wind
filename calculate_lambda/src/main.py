#!/usr/bin/env python3
import os
import json
from time import time
import traceback

import xarray as xr
import boto3

from dotenv import load_dotenv

from power import get_power
from sed import get_sed

load_dotenv()

s3 = boto3.client('s3')

s3_bucket = 'tori-calculate-wind-power'

input_bucket_folder = 'World_2019'
output_main = 'output'
output_compressed = 'output_compressed'


def combine_dict(dict_1: dict, dict_2: dict) -> dict:
    output = {}
    for key, val in dict_1.items():
        output[key] = val, dict_2[key]
    return output


def process_file(file_path, output_all_columns=False) -> None:
    input_file_path = os.path.join('/tmp', os.path.basename(file_path))

    remote_input_file_path = os.path.join(input_bucket_folder, file_path)
    s3.download_file(s3_bucket, remote_input_file_path, input_file_path)

    # first read the MERRA data

    start_time = time()
    with xr.open_mfdataset(input_file_path, combine='by_coords') as ds_wind:
        df_main = ds_wind.to_dataframe()

    print('time open df', time() - start_time)
    print('df size', len(df_main))

    # get data
    output_df = get_power(df_main, output_all_columns)
    start_time = time()
    sed_df = get_sed(df_main)
    print('time calculate sed', time() - start_time)

    output_df['sed'] = sed_df['sed']
    output_df = output_df.groupby(['lat', 'lon'])[
        ['sed', 'power_output']].sum()

    output_file_path = input_file_path
    output_directory = os.path.dirname(output_file_path)
    if not os.path.exists(output_directory):
        os.makedirs(output_directory)

    print(output_df.head())
    print(output_df.shape)
    xr.Dataset.from_dataframe(output_df).to_netcdf(path=output_file_path)

    output_bucket_folder = output_main if output_all_columns else output_compressed
    # save to remote
    remote_output_file_path = os.path.join(output_bucket_folder, file_path)
    s3.upload_file(output_file_path, s3_bucket, remote_output_file_path)

    # remove output file
    if input_file_path != output_file_path:
        os.remove(output_file_path)

    # remove file
    os.remove(input_file_path)


def lambda_handler(event, _context=None):
    file_paths = event['files']
    compressed = 'compressed' in event and event['compressed']
    print(f'lambda start {file_paths[0]} end {file_paths[-1]}')

    for i, file_path in enumerate(file_paths):
        file_name = os.path.basename(file_path)
        country_name = os.path.basename(os.path.dirname(file_path))
        print(
            f'processing file {i + 1}, {file_name} for country {country_name}')
        try:
            start_time = time()
            process_file(file_path, not compressed)
            print('time to process', time() - start_time)
        except Exception:
            print(
                f'error processing file: {file_path}', traceback.format_exc())

    return {
        'statusCode': 200,
        'body': json.dumps({
            'message': f'done processing for {len(file_paths)} files'
        })
    }


if __name__ == '__main__':
    lambda_handler({
        'compressed': True,
        'files': ['Germany/MERRA2_400.tavg1_2d_slv_Nx.20190109.nc4']
        # 'files': ['United States/MERRA2_400.tavg1_2d_slv_Nx.20191022.nc4']
    })
