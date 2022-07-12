#!/usr/bin/env python3
import os
from typing import List

import boto3
import json
import yaml

from dotenv import load_dotenv
from click import confirm
from tqdm import tqdm

load_dotenv()

s3 = boto3.client('s3')
lmda = boto3.client('lambda')

lambda_function = 'tori-wind-lambda'
s3_bucket = 'tori-wind-reduce'
output_folder = 'output_aggregated'
state_folder = os.path.join(output_folder, 'state')

data_folder = os.path.abspath(os.path.join(
    os.path.dirname(__file__), '../data', 'World_2019', '11111'))


def get_s3_keys(prefix='') -> List[str]:
    paginator = s3.get_paginator('list_objects_v2')
    for page in tqdm(paginator.paginate(Bucket=s3_bucket, Prefix=prefix)):
        for content in page.get('Contents', ()):
            yield content['Key']


def main(use_filesystem=True, country_names=[], exclude_country_names=[],
         check_countries_complete=False, max_lambdas=-1):
    country_names = set(country_names)
    exclude_country_names = set(exclude_country_names)

    if use_filesystem:
        all_countries = os.listdir(data_folder)
    else:
        file_path = os.path.join(os.path.dirname(__file__), 'countries.yml')
        with open(file_path, 'r') as f:
            all_countries = yaml.safe_load(f)

    if len(country_names) > 0:
        new_all_countries = []
        for country_name in all_countries:
            if country_name in country_names:
                new_all_countries.append(country_name)
        all_countries = new_all_countries

    if len(exclude_country_names) > 0:
        new_all_countries = []
        for country_name in all_countries:
            if country_name not in country_names:
                new_all_countries.append(country_name)
        all_countries = new_all_countries

    if check_countries_complete:
        print('check completed countries...')
        new_all_countries = []
        state_folders = list(get_s3_keys(state_folder))
        state_countries = {key.split(state_folder)[
            1][1:] for key in state_folders}
        for country in all_countries:
            if country not in state_countries:
                new_all_countries.append(country)

        for state_key in state_folders:
            raw_data = s3.get_object(Bucket=s3_bucket, Key=state_key)['Body'].read()
            completed_files = yaml.safe_load(raw_data)
            if len(completed_files) < 365:
                country_name = state_key.split(state_folder)[1][1:]
                new_all_countries.append(country_name)

        all_countries = new_all_countries

    all_countries.sort()

    print('num countries / lambdas:', len(all_countries))

    # print(all_countries)

    if not confirm('Do you want to continue?', default=True):
        exit()

    for i, country_name in all_countries:
        payload = json.dumps({
            'country': country_name,
            'compressed': True
        })
        response = lmda.invoke(
            FunctionName=lambda_function,
            InvocationType='Event',
            Payload=payload
        )
        print(response)

        if i == max_lambdas - 1:
            break


if __name__ == '__main__':
    main(65, check_file_complete=True, compressed=True,
         use_filesystem=False,
         #  exclude_folder_names=[
         #      'United States', 'China', 'Canada', 'Russia'],
         folder_names=['Germany'],
         max_lambdas=100)
