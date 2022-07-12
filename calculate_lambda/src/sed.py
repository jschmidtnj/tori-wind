#!/usr/bin/env python3
from typing import Dict, Tuple

import pandas as pd
import numpy as np
import xarray as xr

import Humidity_Calculations as HC


def get_sed(df: pd.DataFrame) -> pd.DataFrame:
    df['RH2M'] = df.apply(
        lambda x: HC.convert_SH_to_RH(x.QV2M, x.PS, x.T2M), axis=1)
    df['T2M'] = df['T2M'] - 273.15
    df = HC.Poly_Fit_Optimized_Energy(
        df, 'T2M', 'RH2M', 'sed')

    return df


if __name__ == '__main__':
    input_file_path = '../data/World_2019/11111/Afghanistan/MERRA2_400.tavg1_2d_slv_Nx.20190101.nc4'
    with xr.open_mfdataset(input_file_path, combine='by_coords') as ds_wind:
        df_main = ds_wind.to_dataframe()

    df_sed = get_sed(df_main)
    print(df_sed)
