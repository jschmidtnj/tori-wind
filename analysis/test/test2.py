#!/usr/bin/env python3

import xarray as xr
import os
import numpy as np
import pandas as pd
from time import time
from energy_calc_func import energy_calc

# input_file_path = os.path.abspath(os.path.join(
#     os.path.dirname(__file__), '../../data/World_2019/11111/Afghanistan/MERRA2_400.tavg1_2d_slv_Nx.20190101.nc4'))
input_file_path = os.path.abspath(os.path.join(
     os.path.dirname(__file__), '../../data/World_2019/11111/United States/MERRA2_400.tavg1_2d_slv_Nx.20190102.nc4'))

start_time = time()
with xr.open_mfdataset(input_file_path, combine='by_coords') as ds_wind:
    df_main = ds_wind.to_dataframe()

print(df_main)
print('time to open', time() - start_time)

start_time = time()
new_col10 = np.sqrt(df_main['V10M']**2 + df_main['U10M']**2)
print('time wind 1', time() - start_time)

start_time = time()
new_col50 = np.sqrt(df_main['V50M']**2 + df_main['U50M']**2)
print('time wind 2', time() - start_time)

df_main['wind_speed'] = new_col10
df_main['wind_speed_1'] = new_col50

# sort by column name
df_main = df_main.reindex(sorted(df_main.columns), axis=1)

# rename columns in MERRA data and delete unnecessary data and create a new row for height
df_main.rename(columns={
    'PS': 'pressure', 'T2M': 'temperature',
    'T10M': 'temperature', 'DISPH': 'roughness_length',
    'wind_speed_1': 'wind_speed'
}, inplace=True)
df_main.drop(['TS', 'V10M', 'U50M', 'U10M',
                'V50M', 'QV2M', 'QV10M'
                ], axis=1, inplace=True)

column_data = [
    ['roughness_length', 'pressure', 'temperature',
        'temperature', 'wind_speed', 'wind_speed'],
    [0, 0, 10, 2, 10, 50]
]

column_tuples = list(zip(*column_data))

columns = pd.MultiIndex.from_tuples(
    column_tuples, names=['variable_name', 'height'])

df_main.columns = columns

start_time = time()
sep_latlon_data = {}

for index, row in df_main.iterrows():
    latlon = (index[1], index[2])
    timestamp = index[0]

    if latlon in sep_latlon_data:
        sep_latlon_data[latlon][0].append(timestamp)
        sep_latlon_data[latlon][1].append(row.values)
    else:
        sep_latlon_data[latlon] = [timestamp], [row.values]

# this is a dictionary that will be filled with point df
sep_latlon = {}

for latlon, (timestamps, data) in sep_latlon_data.items():
    sep_latlon[latlon] = pd.DataFrame(
        data, columns=columns, index=timestamps)

print('time sep latlon', time() - start_time)
print('sep latlon size', len(sep_latlon))

start_time = time()
power_data = {}

output_col = 'power_output'

# calculating the energy output for each point
# { (40.32, 93.43): pd.DataFrame() } = sep_latlon
# [((40.32, 93.43), pd.DataFrame(1)), ((39.23, ...))]
for latlong, df in sep_latlon.items():
    power_output = energy_calc(df)

    df[output_col] = power_output
    power_data[latlong] = power_output

print('calculate power time', time() - start_time)

all_df = []
for latlon, df in sep_latlon.items():
    df = df[[output_col]]
    latlon_arr = [[el] * len(df.index) for el in latlon]
    tuples = zip(df.index, latlon_arr[0], latlon_arr[1])
    index = pd.MultiIndex.from_tuples(tuples, names=[
        'time', 'latitude', 'longitude'])
    df.set_index(index, inplace=True)
    all_df.append(df)
output_df = pd.concat(all_df)
output_df.columns = [output_col]
output_df.sort_index(axis=1, inplace=True)

print(output_df)

output_df.to_csv('test2.csv')
