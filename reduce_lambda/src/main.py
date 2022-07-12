#!/usr/bin/env python3
import os

# TODO - download, concat, output

import pandas as pd
import boto3

from dotenv import load_dotenv

load_dotenv()

s3 = boto3.client('s3')

s3_bucket = 'tori-calculate-wind-power'
output_main = 'output'

epsilon = 1e-6

emissions = 1227780.21 # kg


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
    df['sed'] = df['sed'] / (365 * 24)
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
    df = download_database()
    print(df.head(5))
    print(df.shape)
    save_s3(df)


if __name__ == '__main__':
    main()
