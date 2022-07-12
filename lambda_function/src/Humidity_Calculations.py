# -*- coding: utf-8 -*-
"""
Created on Sat Jan  8 16:07:07 2022

@author: schulte
"""

import os
import pandas as pd
import pickle
import math


def convert_SH_to_RH(sh, p_ambient, T_ambient):
    '''
    Converts specific to relative humidity.
    Input: 
        T_ambient in Kelvin
        p_ambient in Pascals
    '''
    # 0 Reference Point and Constants
    T_0 = 273.15  # In [K]
    R_d = 287.058  # Dry Air, in [J/(kg*K)]
    R_v = 461.5  # Water Vapor, in [J/(kg*K)]

    # 1 Calculate vapor mass fraction in dry air from specific humidity
    #
    # Formula derived from definition of specific humidity
    mass_fraction = 1/(1/sh - 1)

    # 2 Calculate saturation vapor mass fraction;

    # Utilizes deal Gas and Clausis-Clapeyron equation for water to calculate vapor pressure depending on temperature
    #
    # 2.1 Clausis-Clapeyron
    saturated_vapor_pressure = 611 * \
        math.exp((17.67*(T_ambient - T_0)) / (T_ambient-29.65))

    # 2.2 calculate saturation mass ratio
    saturation_mass_fraction = (
        saturated_vapor_pressure*R_d) / ((p_ambient-saturated_vapor_pressure)*R_v)

    # 3 Relative humidity
    RH = mass_fraction/saturation_mass_fraction

    return RH

with open(os.path.join(os.path.dirname(__file__), './model.pkl'), 'rb') as f:
    model = pickle.load(f)

with open(os.path.join(os.path.dirname(__file__), './poly.pkl'), 'rb') as f:
    poly = pickle.load(f)

def Poly_Fit_Optimized_Energy(df, T_name, phi_name, res_name):
    '''
    Input: Dataframe with Ambient Temperature and Relative Humidity Column
           Column Names 
           Desired result column name

    Output: Dataframe with results column
    '''

    # 1 Data Preparation
    T_amb = df[T_name]  # read out column
    phi_amb = df[phi_name]  # read out column

    X = pd.DataFrame({'T_amb': T_amb, 'phi_amb': phi_amb}
                     )  # create dataframe for prediction
    X['T_amb'] = X['T_amb'] + 273.15  # Convert [°C] to [K]
    X['phi_amb'] = X['phi_amb'] * 100  # Convert to [%]

    poly_features = poly.fit_transform(X)  # transform to polynomial shape

    # 2 Predict
    y_pred = model.predict(poly_features)

    # 3 Write result to df
    df[res_name] = y_pred

    return df
