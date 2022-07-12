#!/usr/bin/env python3
from typing import Dict, Tuple, Union
import numpy as np
import pandas as pd
import xarray as xr
from time import time
from energy_calc_func import energy_calc


def get_power(df_main: pd.DataFrame, output_dict: bool = False,
              output_all_columns: bool = True) -> Union[pd.DataFrame, Dict[Tuple[float, float], pd.Series]]:
    '''
    this function translates merra data, seperates each latitude 
    and longitude, and calculates the power output for each 
    location

    @param df_main: pd.DataFrame
    @param output_dict: bool
    @returns power_data: Dict[Tuple[float, float], pd.Series]
    '''
    output_df = df_main.copy() if output_all_columns else None

    # find the exact wind speed at 10 and 50m and put it back into df
    # sqrt((V50M)^2+(U50M)^2)=wind_speed
    # sqrt((V10M)^2+(U10M)^2)=wind_speed

    def get_mag_windspeed(row, height):
        a = row[f'V{height}M']
        b = row[f'U{height}M']
        wind_speed = np.sqrt(a**2 + b**2)
        return wind_speed

    start_time = time()
    new_col10 = df_main.apply(lambda row: get_mag_windspeed(row, 10), axis=1)
    print('time wind 1', time() - start_time)

    start_time = time()
    new_col50 = df_main.apply(lambda row: get_mag_windspeed(row, 50), axis=1)
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

    # seperate data by lat and lon so each set of data is for a specific point

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

    if output_dict:
        return power_data

    start_time = time()

    if output_df is not None:
        output_df[output_col] = np.zeros(len(output_df))

    output_data = {}

    for latlon, df in sep_latlon.items():
        for timestamp, row in df.iterrows():
            if output_df is not None:
                output_df.at[(timestamp, latlon[0], latlon[1]),
                             output_col] = row[output_col]
            else:
                output_data[(timestamp, latlon[0], latlon[1])
                            ] = row[output_col].item()

    if output_df is None:
        index = pd.MultiIndex.from_tuples(list(output_data.keys()), names=[
            'time', 'latitude', 'longitude'])
        output_df = pd.DataFrame(
            list(output_data.values()), index=index, columns=[output_col])

    print('add to output time', time() - start_time)

    return output_df


if __name__ == '__main__':
    input_file_path = '../data/World_2019/11111/United States/MERRA2_400.tavg1_2d_slv_Nx.20190101.nc4'
    with xr.open_mfdataset(input_file_path, combine='by_coords') as ds_wind:
        df_main = ds_wind.to_dataframe()
    get_power(df_main)
