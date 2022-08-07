from numpy import NaN
import pandas as pd

columns = ['City', 'Time', 'PM2.5', 'PM10', 'NO2', 'CO', 'SO2', 'O3', 'Temp', 'RHum', 'WSpd', 'O3 P1', 'O3 P4', 'O3 P8', 'O3 P24', 'PM2.5 P1', 'PM2.5 P4', 'PM2.5 P8', 'PM2.5 P24']
data = [['Denver', '2020-01-01 01:00:00+00:00', 0.001, NaN, 40.0, 1.3, 0.002, 0.048, 0.1, 41.0, 11.2, 0, 1, 2, 3, 4, 5, 6, 7]]

df2 = pd.DataFrame(data, columns= columns)
print(df2)