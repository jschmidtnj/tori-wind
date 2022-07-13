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

lambda_function = 'tori-wind-reduce-lambda'
s3_bucket = 'tori-calculate-wind-power'
output_folder = 'output_aggregated'

data_folder = os.path.abspath(os.path.join(
    os.path.dirname(__file__), '../../data', 'World_2019', '11111'))


def get_s3_keys(prefix='') -> List[str]:
    paginator = s3.get_paginator('list_objects_v2')
    for page in tqdm(paginator.paginate(Bucket=s3_bucket, Prefix=prefix)):
        for content in page.get('Contents', ()):
            yield content['Key']


def main(use_filesystem=True, country_names=[], exclude_country_names=[],
         check_country_complete=False, max_lambdas=-1, run_local=False,
         compressed=False):
    country_names = set(country_names)
    exclude_country_names = set(exclude_country_names)

    if use_filesystem:
        all_countries = os.listdir(data_folder)
    else:
        file_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../countries.yml'))
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

    if check_country_complete:
        print('check completed countries...')
        new_all_countries = []
        output_keys = list(get_s3_keys(output_folder))
        all_output_keys = {key.split(output_folder)[
            1][1:] for key in output_keys}

        all_countries = [file_name for file_name in all_countries
                         if file_name not in all_output_keys]

    all_countries.sort()

    run_local = confirm('Run locally?', default=run_local)

    print('num countries / lambdas:', len(all_countries))

    if len(all_countries) == 0:
        print('no countries')
        return

    if confirm('Show countries?', default=False):
        print(all_countries)

    if not confirm('Do you want to continue?', default=True):
        exit()

    if run_local:
        from main import lambda_handler

    for i, country_name in enumerate(all_countries):
        payload = json.dumps({
            'country': country_name,
            'compressed': compressed
        })
        if run_local:
            response = lambda_handler(json.loads(payload), local=run_local)
        else:
            response = lmda.invoke(
                FunctionName=lambda_function,
                InvocationType='Event',
                Payload=payload
            )
        print(response)

        if i == max_lambdas - 1:
            break


if __name__ == '__main__':
    main(check_country_complete=True, compressed=True,
         use_filesystem=False,
         country_names=['Germany'],
         run_local=True)
