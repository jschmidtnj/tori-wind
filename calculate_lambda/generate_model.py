#!/usr/bin/env python3

# Sklearn Imports
from sklearn.linear_model import LinearRegression
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error, r2_score
from sklearn.preprocessing import PolynomialFeatures


#Standard imports
import numpy as np


import os
import pandas as pd


#####################



#1 Load model results from pickle file
opt_model_data = pd.read_pickle(os.path.join(os.path.dirname(__file__), './DAC_Opt.pkl'))
opt_model_data['Energy_Demand'] = opt_model_data.spec_W_fan + opt_model_data.spec_W_vac + (opt_model_data.spec_Q_H / opt_model_data.COP)

data = opt_model_data

######################## Scatter Plot Optimization Results
#fig, (ax0) = plt.subplots(nrows = 1, 
#                                    sharex=True, 
#                                    figsize=(18/2.54, 13/2.54))



#ax0.scatter(data['phi_amb'].loc[data['T_amb'] < 253.16],data['FitnessValue'].loc[data['T_amb'] < 260.16]/1e6, label = '-20 °C')
#ax0.scatter(data['phi_amb'].loc[data['T_amb'] == 263.15],data['FitnessValue'].loc[data['T_amb'] == 263.15]/1e6, label = '-10 °C')
#ax0.scatter(data['phi_amb'].loc[data['T_amb'] == 273.15],data['FitnessValue'].loc[data['T_amb'] == 273.15]/1e6, label = '0 °C')
#ax0.scatter(data['phi_amb'].loc[data['T_amb'] == 283.15],data['FitnessValue'].loc[data['T_amb'] == 283.15]/1e6, label = '10 °C')
#ax0.scatter(data['phi_amb'].loc[data['T_amb'] == 293.15],data['FitnessValue'].loc[data['T_amb'] == 293.15]/1e6, label = '20 °C')
#ax0.scatter(data['phi_amb'].loc[data['T_amb'] == 303.15],data['FitnessValue'].loc[data['T_amb'] == 303.15]/1e6, label = '30 °C')

#ticks = ticker.FuncFormatter(lambda x, pos: '{0:g}'.format(x*1e-6))
#ax.yaxis.set_major_formatter(ticks)

#########################


#2 Define X and y and perform train test split
X = opt_model_data[['T_amb','phi_amb']].reset_index().drop(columns="index")
y = opt_model_data[['Energy_Demand']]

X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.1, random_state=1)


#3 Define and train polynomial model
poly = PolynomialFeatures(degree=8, include_bias=False) # 
poly_features = poly.fit_transform(X) # Create Polynomial features

model = LinearRegression() # Create mechine learning model
model.fit(poly_features,y)



########################## Testing 

#1 Test model with training data
prediction = model.predict(poly_features)
prediction_df = pd.DataFrame(data = prediction, columns = ["prediction"])
prediction_df = pd.concat([prediction_df, X], axis = 1)
data=prediction_df

#ax0.plot(data['phi_amb'].loc[data['T_amb'] < 253.16],data['prediction'].loc[data['T_amb'] < 260.16]/1e6, label = '-20 °C')
#ax0.plot(data['phi_amb'].loc[data['T_amb'] == 263.15],data['prediction'].loc[data['T_amb'] == 263.15]/1e6, label = '-10 °C')
#ax0.plot(data['phi_amb'].loc[data['T_amb'] == 273.15],data['prediction'].loc[data['T_amb'] == 273.15]/1e6, label = '0 °C')
#ax0.plot(data['phi_amb'].loc[data['T_amb'] == 283.15],data['prediction'].loc[data['T_amb'] == 283.15]/1e6, label = '10 °C')
#ax0.plot(data['phi_amb'].loc[data['T_amb'] == 293.15],data['prediction'].loc[data['T_amb'] == 293.15]/1e6, label = '20 °C')
#ax0.plot(data['phi_amb'].loc[data['T_amb'] == 303.15],data['prediction'].loc[data['T_amb'] == 303.15]/1e6, label = '30 °C')

#2 Visualization
#plt.plot(opt_model_data.phi_amb.iloc[0:10],prediction_df.iloc[0:10,0]) # Plot prediction
#plt.plot(opt_model_data.phi_amb.iloc[10:20],prediction_df.iloc[10:20,0]) # Plot prediction
#ax0.legend(loc = "upper right")

#3 Metrics 
rmse = np.sqrt(mean_squared_error(y,prediction)) # RMSE
nrmse = rmse / opt_model_data['Energy_Demand'].mean() # Normalize RMSE
print(rmse)
print(nrmse)

r2 = r2_score(y,prediction) # R2 value
print(r2)

print(model.coef_)

import pickle

output_folder = os.path.join(os.path.dirname(__file__), 'src')

with open(os.path.join(output_folder, 'model.pkl'), 'wb') as f:
    pickle.dump(model, f)

with open(os.path.join(output_folder, 'poly.pkl'), 'wb') as f:
    pickle.dump(poly, f)
