#!/usr/bin/env python3
import os
import atexit

import pandas as pd
import boto3

from dotenv import load_dotenv
from pymongo import MongoClient

load_dotenv()

s3 = boto3.client('s3')

s3_bucket = 'tori-calculate-wind-power'
output_main = 'output'

epsilon = 1e-6

# TODO - figure out what emissions should be
emissions = 1e6

mongo_client: MongoClient = None


def close_mongo():
    mongo_client.close()


def initialize_mongo_connection():
    global mongo_client
    mongo_uri = os.getenv('MONGO_URI')
    if not mongo_uri:
        raise ValueError('cannot find mongo uri')

    mongo_client = MongoClient(mongo_uri)

    atexit.register(close_mongo)


def download_database() -> pd.DataFrame:
    db = mongo_client.wind
    collection = db.wind

    df = pd.DataFrame(list(collection.find({})))

    coordinates = [point['coordinates'] for point in df['location'].values]
    coordinates = [(coord[1], coord[0]) for coord in coordinates]
    index = pd.MultiIndex.from_tuples(coordinates, names=['lat', 'lon'])
    df.set_index(index, inplace=True)
    df.drop(['location'], axis=1, inplace=True)

    df['power_output'] = df.apply(
        lambda x: x['power_output'] + epsilon, axis=1)
    df['carbon_footprint'] = df.apply(
        lambda x: emissions / (x['power_output'] * 20 / 1000), axis=1)
    df['emissions_carbon_capture'] = df.apply(
        lambda x: x['carbon_footprint'] * x['sed'], axis=1)

    return df


def save_s3(df: pd.DataFrame) -> None:
    output_file_path = os.path.join('/tmp', 'all_data.csv')
    df.to_csv(output_file_path)

    remote_output_file_path = os.path.join(
        output_main, os.path.basename(output_file_path))

    s3.upload_file(output_file_path, s3_bucket, remote_output_file_path)


def main() -> None:
    initialize_mongo_connection()
    df = download_database()
    print(df.head(5))
    print(df.shape)
    save_s3(df)


if __name__ == '__main__':
    main()
