#!/usr/bin/env python3
from typing import Dict, Tuple, Union
import numpy as np
import pandas as pd
import xarray as xr
from time import time
from energy_calc_func import energy_calc

roughness_length = 0.15


def get_power(df_main: pd.DataFrame,
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
        'T10M': 'temperature', 'wind_speed_1': 'wind_speed'
    }, inplace=True)
    df_main['roughness_length'] = [roughness_length] * len(df_main)
    df_main.drop(['TS', 'V10M', 'U50M', 'U10M',
                  'V50M', 'QV2M', 'QV10M', 'DISPH',
                  ], axis=1, inplace=True)

    column_data = [
        ['pressure', 'temperature', 'temperature',
         'wind_speed', 'wind_speed', 'roughness_length'],
        [0, 10, 2, 10, 50, 0]
    ]

    column_tuples = list(zip(*column_data))

    columns = pd.MultiIndex.from_tuples(
        column_tuples, names=['variable_name', 'height'])

    df_main.columns = columns

    power_output = energy_calc(df_main)

    print('calculate power time', time() - start_time)

    output_col = 'power_output'

    start_time = time()

    if output_df is not None:
        output_df[output_col] = power_output.values
    else:
        output_df = power_output.to_frame(name=output_col)

    print('add to output time', time() - start_time)

    return output_df


if __name__ == '__main__':
    input_file_path = './data/input/United States/MERRA2_400.tavg1_2d_slv_Nx.20190101.nc4'
    with xr.open_mfdataset(input_file_path, combine='by_coords') as ds_wind:
        df_main = ds_wind.to_dataframe()
    df = get_power(df_main, output_all_columns=False)
    print(df.head())
