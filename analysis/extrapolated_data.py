# find power data for one location for one year
import os
import pandas as pd
import xarray as xr
from glob import glob

def organize_data(folder_path):
    files_in_folder = os.path.join(folder_path, '*.nc4')

    print('get df')
    with xr.open_mfdataset(files_in_folder, combine='by_coords') as ds_wind:
        df = ds_wind.to_dataframe()
    print('parsed into df')

    dic_year = {}

    for index, row in df.iterrows():
        latlon = (index[1], index[2])
        if latlon not in dic_year:
            dic_year[latlon] = row['power_output']
        else:
            dic_year[latlon] = dic_year[latlon]+row['power_output']

    index = pd.MultiIndex.from_tuples(
        list(dic_year.keys()), names=['lat', 'lon'])
    df_year = pd.DataFrame(
        {'yearly power': list(dic_year.values())}, index=index)

    return df_year

def extrapolate_all_data():
    data_folder = os.path.abspath(os.path.join(
        os.path.dirname(__file__), '../data/cloud_output'))

    # all_folders is a list of paths to folders (string) -> List[str]
    all_folders = sorted(glob(os.path.join(data_folder, '**')))

    for folder_path in all_folders:
        output_path = os.path.abspath(os.path.join(os.path.dirname(
            __file__), '../data', 'Power_2019', os.path.basename(folder_path) + '.csv'))
        organize_data(folder_path).to_csv(output_path)

if __name__ == '__main__':
    extrapolate_all_data()
