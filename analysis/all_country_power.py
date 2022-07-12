import os
import numpy as np
from time import time
from glob import glob
from power import getpower

# make -1 to run all files
num_files = 1

# 456 locations per file
# 365 files per folder
# 251 folders

data_folder = os.path.abspath(os.path.join(
    os.path.dirname(__file__), '../data/World_2019'))

output_folder = os.path.abspath(
    os.path.join(os.path.dirname(__file__), '../data/output'))

all_files = glob(os.path.join(data_folder, '**/*.nc4'))
all_files.sort()
print(len(all_files))

benchmark = []

for input_file_path in all_files:
    file_name = os.path.basename(input_file_path)
    country_name = os.path.basename(os.path.dirname(input_file_path))
    print(f'processing file {file_name} for country {country_name}')

    output_file_path = os.path.abspath(os.path.join(
        output_folder, input_file_path.split(data_folder)[1][1:]))

    start_time = time()
    power = getpower(input_file_path, output_file_path)
    calc_duration = time() - start_time
    benchmark.append(calc_duration)

    print('average time:', np.average(benchmark))

    if num_files == len(benchmark):
        break
