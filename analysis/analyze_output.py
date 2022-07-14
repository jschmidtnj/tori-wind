#!/usr/bin/env python3

from collections import Counter
from typing import List
from tqdm import tqdm
import boto3
import os

s3_bucket = 'tori-calculate-wind-power'
bucket_folder = 'output_compressed'

s3 = boto3.client('s3')


def get_keys(prefix='') -> List[str]:
    paginator = s3.get_paginator('list_objects_v2')
    for page in tqdm(paginator.paginate(Bucket=s3_bucket, Prefix=prefix)):
        for content in page.get('Contents', ()):
            yield content['Key']


def count_files():
    all_keys = list(get_keys(bucket_folder))
    print('num keys:', len(all_keys))

    count = Counter()
    for key in all_keys:
        folder_name = key.split(bucket_folder)[1].split(os.sep)[1]
        count[folder_name] += 1
    # print('folder names:', list(count.keys()))
    print('counts:', count.items())
    target = 365
    num_target = sum([1 for _name, cnt in count.items() if cnt == target])
    print('num target:', num_target)
    remaining = [(name, cnt) for name, cnt in count.items() if cnt < target]
    print('remaining:', len(remaining))
    print(remaining)
    print([name for name, _cnt in remaining])


if __name__ == '__main__':
    count_files()
