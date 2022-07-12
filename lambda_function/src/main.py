#!/usr/bin/env python3
import os
import json
from time import time
from typing import Dict, List, Tuple
import traceback

import xarray as xr
import pandas as pd
import boto3

from dotenv import load_dotenv
from pymongo import MongoClient, UpdateOne, InsertOne, GEOSPHERE

from power import get_power
from sed import get_sed, get_sed_hourly

load_dotenv()

s3 = boto3.client('s3')

s3_bucket = 'tori-calculate-wind-power'

input_bucket_folder = 'World_2019'
output_main = 'output'
output_compressed = 'output_compressed'

mongo_client: MongoClient = None


def close_mongo():
    mongo_client.close()


def initialize_mongo_connection():
    global mongo_client
    mongo_uri = os.getenv('MONGO_URI')
    if not mongo_uri:
        raise ValueError('cannot find mongo uri')

    mongo_client = MongoClient(mongo_uri)


def combine_dict(dict_1: dict, dict_2: dict) -> dict:
    output = {}
    for key, val in dict_1.items():
        output[key] = val, dict_2[key]
    return output


def save_to_database(data: Dict[Tuple[float, float], Tuple[pd.Series, pd.Series]],
                     file_paths: List[str]) -> None:
    db = mongo_client.wind

    collection = db.wind

    print('running mongodb')

    collection.create_index([('location', GEOSPHERE)], unique=True)

    commands = []
    for (lat, lng), (power_output, sed) in data.items():
        total_power_output = power_output.sum()
        total_sed = sed.sum()

        command = UpdateOne({
            'location': {
                '$eq': {
                    'type': 'Point',
                    'coordinates': [lng, lat]
                }
            }
        }, {
            '$inc': {
                'power_output': total_power_output,
                'sed': total_sed
            }
        }, True)
        commands.append(command)

    start_time = time()
    result = collection.bulk_write(commands)
    print('time write data', time() - start_time)
    print('num modified:', result.modified_count)

    collection = db.completed_files
    commands = []
    for file_path in file_paths:
        country_name = file_path.split(os.sep)[0]
        command = InsertOne({
            'name': file_path,
            'country': country_name
        })
        commands.append(command)

    start_time = time()
    result = collection.bulk_write(commands)
    print('time write files', time() - start_time)
    print('num added:', result.inserted_count)


def process_file(file_path, output_all_columns=False,
                 save_db=True) -> Dict[Tuple[float, float], Tuple[pd.Series, pd.Series]]:
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
    data = get_power(df_main, save_db, output_all_columns)
    df_main = get_sed(df_main)

    if save_db:
        sed_data = get_sed_hourly(df_main)
        data = combine_dict(data, sed_data)

        # remove file
        os.remove(input_file_path)

        return data

    output_file_path = input_file_path
    output_directory = os.path.dirname(output_file_path)
    if not os.path.exists(output_directory):
        os.makedirs(output_directory)

    xr.Dataset.from_dataframe(df_main).to_netcdf(path=output_file_path)

    output_bucket_folder = output_main if output_all_columns else output_compressed
    # save to remote
    remote_output_file_path = os.path.join(output_bucket_folder, file_path)
    s3.upload_file(output_file_path, s3_bucket, remote_output_file_path)

    # remove output file
    if input_file_path != output_file_path:
        os.remove(output_file_path)

    # remove file
    os.remove(input_file_path)

    return data


def lambda_handler(event, _context=None):
    save_file_key = 'save_file'
    save_db = not (save_file_key in event and event[save_file_key])

    file_paths = event['files']
    print(f'lambda start {file_paths[0]} end {file_paths[-1]}')

    data = {}

    for i, file_path in enumerate(file_paths):
        file_name = os.path.basename(file_path)
        country_name = os.path.basename(os.path.dirname(file_path))
        print(
            f'processing file {i + 1}, {file_name} for country {country_name}')
        try:
            curr_data = process_file(file_path, False, save_db)
            for key, curr_val in curr_data.items():
                val = data.get(key, (0., 0.))
                data[key] = (val[0] + curr_val[0].sum(),
                             val[1] + curr_val[1].sum())
        except Exception:
            print(
                f'error processing file: {file_path}', traceback.format_exc())

    if save_db:
        initialize_mongo_connection()

        save_to_database(data, file_paths)

        mongo_client.close()

    return {
        'statusCode': 200,
        'body': json.dumps({
            'message': f'done processing for {len(file_paths)} files'
        })
    }


if __name__ == '__main__':
    lambda_handler({
        # 'files': ['Germany/MERRA2_400.tavg1_2d_slv_Nx.20190109.nc4']
        'files': ['United States/MERRA2_400.tavg1_2d_slv_Nx.20191022.nc4']
    })
