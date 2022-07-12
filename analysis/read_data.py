import os
import xarray as xr
import netCDF4 as nc

file_path = os.path.join(os.path.dirname(__file__),
                         '../data/output/Afghanistan/MERRA2_400.tavg1_2d_slv_Nx.20190101.nc4')
ds = nc.Dataset(file_path)
print(ds)

with xr.open_mfdataset(file_path, combine='by_coords') as ds:
    df = ds.to_dataframe()
    print(df.head())
    print(df.columns)
