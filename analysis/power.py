#!/usr/bin/env python3
import os
import numpy as np
import pandas as pd
import xarray as xr
from time import time
from energy_calc_func import energy_calc


def getpower(input_file_path: str, output_file_path: str):
    '''
    this function translates merra data, seperates each latitude 
    and longitude, and calculates the power output for each 
    location
    @param input_file_path: str
    @param output_file_path: str
    @returns power_data: Dict[Tuple[float, float], pd.Series]
    '''
    # first read the MERRA data
    # transfering it from 01 to df_wind

    start_time = time()
    with xr.open_mfdataset(input_file_path, combine='by_coords') as ds_wind:
        output_df = ds_wind.to_dataframe()
    print('time open df', time() - start_time)
    df_wind = output_df.copy()
    print('df size', len(df_wind))

    # df_org=df_wind

    # find the exact wind speed at 10 and 50m and put it back into df
    # sqrt((V50M)^2+(U50M)^2)=wind_speed
    # sqrt((V10M)^2+(U10M)^2)=wind_speed

    def get_mag_windspeed(row, height):
        a = row[f'V{height}M']
        b = row[f'U{height}M']
        wind_speed = np.sqrt(a**2 + b**2)
        return wind_speed

    start_time = time()
    new_col10 = df_wind.apply(lambda row: get_mag_windspeed(row, 10), axis=1)
    print('time wind 1', time() - start_time)

    start_time = time()
    new_col50 = df_wind.apply(lambda row: get_mag_windspeed(row, 50), axis=1)
    print('time wind 2', time() - start_time)

    df_wind['wind_speed'] = new_col10
    df_wind['wind_speed_1'] = new_col50

    # sort by column name
    df_wind = df_wind.reindex(sorted(df_wind.columns), axis=1)

    # rename columns in MERRA data and delete unnecessary data and create a new row for height
    df_wind = df_wind.rename(columns={
        'PS': 'pressure', 'T2M': 'temperature',
        'T10M': 'temperature', 'DISPH': 'roughness_length',
        'wind_speed_1': 'wind_speed'
    })
    df_wind = df_wind.drop(['TS', 'V10M', 'U50M', 'U10M',
                            'V50M', 'QV2M', 'QV10M'
                            ], axis=1)

    column_data = [
        ['roughness_length', 'pressure', 'temperature',
         'temperature', 'wind_speed', 'wind_speed'],
        [0, 0, 10, 2, 10, 50]
    ]

    column_tuples = list(zip(*column_data))

    columns = pd.MultiIndex.from_tuples(
        column_tuples, names=["variable_name", "height"])

    # create columns for data
    # dfnew = pd.DataFrame({'variable_name': ['height'], 'pressure': [0], 'temperature': [
    # 2], 'wind_speed': [10], 'roughness_length': [0], 'temperature.1': [10], 'wind_speed': [50]},)

    # seperate data by lat and lon so each set of data is for a specific point

    start_time = time()
    sep_latlon_data = {}

    for index, row in df_wind.iterrows():
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
        # print('energy calc', latlong)
        power_output = energy_calc(df)

        df[output_col] = power_output
        power_data[latlong] = power_output

        # print(df)
        # with pd.option_context('display.max_columns', None):
        #     print(df.head())

    print('calculate power time', time() - start_time)

    start_time = time()

    output_df[output_col] = np.zeros(len(output_df))

    for latlon, df in sep_latlon.items():
        for timestamp, row in df.iterrows():
            output_df.at[(timestamp, latlon[0], latlon[1]), output_col] = row[output_col]

    print('add to output time', time() - start_time)

    output_directory = os.path.dirname(output_file_path)
    if not os.path.exists(output_directory):
        os.makedirs(output_directory)

    xr.Dataset.from_dataframe(output_df).to_netcdf(path=output_file_path)

    return power_data
