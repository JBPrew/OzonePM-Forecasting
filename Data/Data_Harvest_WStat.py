from math import nan
from numpy import NaN
import requests
from pandas import DataFrame, date_range
from datetime import datetime, timedelta
from meteostat import Hourly
import pandas as pd


def data_harvest(city, weatherstationlocal, filename):
    df = DataFrame(columns=["City", "Time", "O3", "PM2.5", "PM10", "CO", "SO2", "NO2", "NH3", "Temp", "RHum", "WSpd"]).astype({"City": str, "Time": str, "O3": float, "PM2.5": float, "PM10": float, "CO": float, "SO2": float, "NO2": float, "NH3": float, "Temp":float, "RHum":float, "WSpd":float})
    
    response = requests.get("http://api.openweathermap.org/data/2.5/air_pollution/history?lat={lat}&lon={lon}&start={start}&end={end}&appid={API}".format(lat = "13.7563", lon = "100.5018", API = "a0a139b51a7aed9183df55c619fca847", start = 1609477200, end = 1641009600)).json()
    print(response["list"])
    
    for i in range(len(response["list"])):
        time = datetime.fromtimestamp(response["list"][i]["dt"])
        
        o3 = response["list"][i]["components"]["o3"]
        pm25 = response["list"][i]["components"]["pm2_5"]
        pm10 = response["list"][i]["components"]["pm2_5"]
        co = response["list"][i]["components"]["co"]
        so2 = response["list"][i]["components"]["so2"]
        no2 = response["list"][i]["components"]["no2"]
        nh3 = response["list"][i]["components"]["nh3"]

        data = Hourly(weatherstationlocal, time, time)
        data = data.fetch()
        if data.empty:
            temp, rhum, wspd = nan, nan, nan
        temp, rhum, wspd = data.iloc[0]["temp"], data.iloc[0]["rhum"], data.iloc[0]["wspd"]
      

        df.loc[len(df)] = [city, time, o3, pm25, pm10, co, so2, no2, nh3, temp, rhum, wspd]


    #interpolate (fill in missing values) and reverse the dataframe into ascending early
    # df = (df.interpolate(axis=0))

    df_future = pd.DataFrame(columns= ["O3_P1", "O3_P4", "O3_P8", "O3_P24", "PM2.5_P1", "PM2.5_P4", "PM2.5_P8", "PM2.5_P24"]).astype({"O3_P1":float, "O3_P4":float, "O3_P8":float, "O3_P24":float,  "PM2.5_P1":float, "PM2.5_P4":float, "PM2.5_P8":float, "PM2.5_P24":float})
    df_future["O3_P1"] = df["O3"].shift(-1)
    df_future["O3_P4"] = df["O3"].shift(-4)
    df_future["O3_P8"] = df["O3"].shift(-8)
    df_future["O3_P24"] = df["O3"].shift(-24)
    df_future["PM2.5_P1"] = df["PM2.5"].shift(-1)
    df_future["PM2.5_P4"] = df["PM2.5"].shift(-4)
    df_future["PM2.5_P8"] = df["PM2.5"].shift(-8)
    df_future["PM2.5_P24"] = df["PM2.5"].shift(-24)
    df = pd.concat([df, df_future], axis=1)
    df = df[:-24]
    df.info()


    df.to_csv("data/" + filename, mode = 'w', index= False)




data_harvest("Bangkok", weatherstationlocal="48455", filename="Bangkok2021WSTAT.csv")
