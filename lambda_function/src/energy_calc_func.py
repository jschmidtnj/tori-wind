#!/usr/bin/env python3
import windpowerlib as wpl

enercon_e82 = {
    "turbine_type": "E-82/2300",  # turbine type as in register
    #https://github.com/wind-python/windpowerlib/blob/master/windpowerlib/oedb/power_curves.csv
    #^to find turbine data
    "hub_height": 138 #78-138,  # in m
}
e82 = wpl.WindTurbine(**enercon_e82)

modelchain_data = {
    "wind_speed_model": "logarithmic",  # 'logarithmic' (default),
    # 'hellman' or
    # 'interpolation_extrapolation'
    "density_model": "ideal_gas",  # 'barometric' (default), 'ideal_gas' or
    # 'interpolation_extrapolation'
    "temperature_model": "linear_gradient",  # 'linear_gradient' (def.) or
    # 'interpolation_extrapolation'
    "power_output_model": "power_coefficient_curve",  # 'power_curve'
    # (default) or 'power_coefficient_curve'
    "density_correction": True,  # False (default) or True
}  # None (default) or None

mc_e82 = wpl.ModelChain(e82, **modelchain_data)

def energy_calc(weather_df) -> float:
    model_data = mc_e82.run_model(weather_df)

    power_output = model_data.power_output

    return power_output
