#!/usr/bin/env python3
import os
from typing import List

import numpy as np
import boto3
import json

from dotenv import load_dotenv
from glob import glob
from click import confirm
from tqdm import tqdm

load_dotenv()

# account lambda limit
lambda_limit = 1100

s3 = boto3.client('s3')
lmda = boto3.client('lambda')

lambda_function = 'tori-wind-lambda'
s3_bucket = 'tori-calculate-wind-power'
output_main = 'output'
output_compressed = 'output_compressed'
input_bucket_folder = 'World_2019'

data_folder = os.path.abspath(os.path.join(
    os.path.dirname(__file__), '../data', 'World_2019', '11111'))


def get_s3_keys(prefix='') -> List[str]:
    paginator = s3.get_paginator('list_objects_v2')
    for page in tqdm(paginator.paginate(Bucket=s3_bucket, Prefix=prefix)):
        for content in page.get('Contents', ()):
            yield content['Key']


def main(num_per_lambda, use_filesystem=True, folder_names=[], exclude_folder_names=[],
         check_file_complete=False, compressed=False, max_lambdas=-1):
    folder_names = set(folder_names)
    exclude_folder_names = set(exclude_folder_names)

    if use_filesystem:
        all_files = glob(os.path.join(data_folder, '**/*.nc4'))
        all_files = [file_path.split(data_folder)[1][1:]
                     for file_path in all_files]
    else:
        prefix = input_bucket_folder
        if len(folder_names) == 1:
            prefix = os.path.join(prefix, list(folder_names)[0])
        all_files = get_s3_keys(prefix)
        all_files = [file_path.split(input_bucket_folder)[1][1:]
                     for file_path in all_files]

    if len(folder_names) > 0:
        new_all_files = []
        for file_path in all_files:
            folder_name = file_path.split(os.sep)[0]
            # print(folder_name)
            if folder_name in folder_names:
                new_all_files.append(file_path)
        all_files = new_all_files

    if len(exclude_folder_names) > 0:
        new_all_files = []
        for file_path in all_files:
            folder_name = file_path.split(os.sep)[0]
            # print(folder_name)
            if folder_name not in exclude_folder_names:
                new_all_files.append(file_path)
        all_files = new_all_files

    if check_file_complete:
        print('check completed files...')
        output_bucket_folder = output_compressed if compressed else output_main
        all_output_keys = {key.split(output_bucket_folder)[
            1][1:] for key in get_s3_keys(output_bucket_folder)}
        # print(all_output_keys)

        all_files = [file_name for file_name in all_files
                     if file_name not in all_output_keys]

    all_files.sort()

    print('num files:', len(all_files))

    num_lambdas = len(all_files) // num_per_lambda

    print('num lambdas:', num_lambdas)

    assert max_lambdas != -1 or num_lambdas <= lambda_limit

    # print(all_files)

    if not confirm('Do you want to continue?', default=True):
        exit()

    for i, curr_files in enumerate(np.array_split(all_files, num_lambdas)):
        curr_files = curr_files.tolist()
        print('curr files', curr_files)

        payload = json.dumps({
            'files': curr_files,
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
    main(1, check_file_complete=True, compressed=True,
         use_filesystem=False,
         #  exclude_folder_names=[
         #      'United States', 'China', 'Canada', 'Russia'],
         folder_names=['United States']
         )
