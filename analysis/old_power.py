#!/usr/bin/env python3
import numpy as np
import pandas as pd
import xarray as xr
from time import time
from energy_calc_func import energy_calc

def getpower(file_path):
    '''
    this function translates merra data, seperates each latitude 
    and longitude, and calculates the power output for each 
    location
    @param file_path: str
    @returns power_data: Dict[Tuple[float, float], pd.Series]
    '''
    # first read the MERRA data
    # transfering it from 01 to df_wind
    #file_path = os.path.join(os.path.dirname(__file__),
                            #'../data/Europe_2019/Austria/MERRA2_400.tavg1_2d_slv_Nx.20190101.nc4')

    with xr.open_mfdataset(file_path, combine='by_coords') as ds_wind:
        df_wind = ds_wind.to_dataframe()

    # df_org=df_wind

    # find the exact wind speed at 10 and 50m and put it back into df
    # sqrt((V50M)^2+(U50M)^2)=wind_speed
    # sqrt((V10M)^2+(U10M)^2)=wind_speed


    def get_mag_windspeed(row, height):
        wind_speed = np.sqrt((row[f'V{height}M'])**2+(row[f'U{height}M'])**2)
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

    # this is a dictionary that will be filled with point df
    sep_latlon = {}

    start_time = time()
    for index, row in df_wind.iterrows():
        latlon = (index[1], index[2])
        timestamp = index[0]

        if latlon in sep_latlon:
            sep_latlon[latlon].loc[timestamp] = row.values
        else:
            sep_latlon[latlon] = pd.DataFrame([row.values], columns=columns,
                                            index=[timestamp])
    print('time sep latlon', time() - start_time)

    power_data = {}

    # calculating the energy output for each point
    # { (40.32, 93.43): pd.DataFrame() } = sep_latlon
    # [((40.32, 93.43), pd.DataFrame(1)), ((39.23, ...))]
    i = 0
    for latlong, df in sep_latlon.items():
        # print('energy calc', latlong)
        power_output = energy_calc(df)

        df['power_output'] = power_output
        power_data[latlong] = power_output
        print(df)
        if i > 10:
            exit()
        i += 1

    # with pd.option_context('display.max_columns', None):
    # print(df_wind.head())

    return power_data
