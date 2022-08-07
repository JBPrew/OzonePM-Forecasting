from numpy import NaN
import requests
from pandas import DataFrame, date_range
from datetime import datetime, timedelta
from meteostat import Hourly
import pandas as pd

def data_collect(location, time, list):
    data = Hourly(location, datetime.fromisoformat(str(time).split('+')[0]), datetime.fromisoformat(str(time).split('+')[0]))
    data = data.fetch()
    if data.empty:
        list.extend([NaN, NaN, NaN])
    else: list.extend([data.iloc[0]["temp"], data.iloc[0]["rhum"], data.iloc[0]["wspd"]])
    
    try:
        hour_ozone = df.loc[0,str(datetime.fromisoformat(time) + timedelta(hours = 1))]
    except:
        hour_ozone = NaN
        print("hour")
    
    try:
        four_hour_ozone = df.loc[0,str(datetime.fromisoformat(time) + timedelta(hours = 4))]
    except:
        four_hour_ozone = NaN
        print("4hour")

    try:
        eight_hour_ozone = df.loc[0,str(datetime.fromisoformat(time) + timedelta(hours = 8))]
    except:
        eight_hour_ozone = NaN
        try:
            print("8hour", time, str(datetime.fromisoformat(time) + timedelta(hours = 8)))
        except:
            print("8bab", time)

    try:
        day_ozone = df.loc[0,str(datetime.fromisoformat(time) + timedelta(hours = 24))]
    except:
        day_ozone = NaN
        print("day")
    
    list.extend([hour_ozone, four_hour_ozone, eight_hour_ozone, day_ozone])

    return list

#columns=["Time", "PM2.5", "PM10", "NO2", "CO", "SO2", "O3", "Temp", "RHum", "WSpd"]
df = DataFrame()
urltemplates = ["https://api.openaq.org/v2/measurements?date_from=2021-10-01T01%3A00%3A00%2B00%3A00&date_to=2022-01-01T01%3A50%3A00%2B00%3A00&limit=100000&page=1&offset=0&sort=desc&parameter=pm10&parameter=pm25&parameter=so2&parameter=o3&parameter=co&parameter=no2&radius=1000&order_by=datetime&isMobile=false&sensorpollutant=reference%20grade&location_id=2532"]
# "https://api.openaq.org/v2/measurements?date_from=2019-07-01T01%3A00%3A00%2B00%3A00&date_to=2019-09-30T01%3A50%3A00%2B00%3A00&limit=100000&page=1&offset=0&sort=desc&parameter=pm10&parameter=pm25&parameter=so2&parameter=o3&parameter=co&parameter=no2&radius=1000&order_by=datetime&isMobile=false&sensorpollutant=reference%20grade&location_id=1265"]
# "https://api.openaq.org/v2/measurements?date_from=2019-04-01T01%3A00%3A00%2B00%3A00&date_to=2019-06-30T01%3A50%3A00%2B00%3A00&limit=100000&page=1&offset=0&sort=desc&parameter=pm10&parameter=pm25&parameter=so2&parameter=o3&parameter=co&parameter=no2&radius=1000&order_by=datetime&isMobile=false&sensorpollutant=reference%20grade&location_id=1265",
# "https://api.openaq.org/v2/measurements?date_from=2019-01-01T01%3A00%3A00%2B00%3A00&date_to=2019-03-31T01%3A50%3A00%2B00%3A00&limit=100000&page=1&offset=0&sort=desc&parameter=pm10&parameter=pm25&parameter=so2&parameter=o3&parameter=co&parameter=no2&radius=1000&order_by=datetime&isMobile=false&sensorpollutant=reference%20grade&location_id=1265"]


for url in urltemplates:
    headers = {"Accept": "application/json"}

    response = requests.get(url, headers=headers)
    datapoints = response.text.split("},{")
    count = 1
    temp_list = [NaN,NaN,NaN,NaN,NaN,NaN]
    current_time = datapoints[0][datapoints[0].index("\"utc\":\"") + len("\"utc\":\""):datapoints[0].index("\",\"local")]
    for elem in datapoints:
        time = elem[elem.index("\"utc\":\"") + len("\"utc\":\""):elem.index("\",\"local")]
        pollutant = elem[elem.index("parameter\":\"") + len("parameter\":\""):elem.index("\",\"value")]
        value = float(elem[elem.index("value\":") + len("value\":"):elem.index(",\"date\"")])
        # print(datetime.fromisoformat(time))
        # print(datetime.fromisoformat(current_time) - timedelta(hours=1))
        if (datetime.fromisoformat(time) == datetime.fromisoformat(current_time) - timedelta(hours=1)):
            temp_list = data_collect(location='KBKF0', time = current_time, list = temp_list)            
            df = pd.concat([df, pd.DataFrame(temp_list, columns = [str(datetime.fromisoformat(current_time))])], axis=1)

            temp_list = [NaN,NaN,NaN,NaN,NaN,NaN]
            current_time = time
        #happens when data is missing for an hour
        elif (time != current_time):
            temp_list = data_collect(location = "KBKF0", time = current_time, list = temp_list)
            df = pd.concat([df, pd.DataFrame(temp_list, columns = [str(datetime.fromisoformat(current_time))])], axis=1)
            temp_list = [NaN,NaN,NaN,NaN,NaN,NaN]

            time_range = date_range(time, current_time, freq="H", inclusive = "neither")
            for hour in time_range:
                data_collect(location='KBKF0', time= str(hour), list = temp_list)        
                df = pd.concat([df, pd.DataFrame(temp_list, columns = [str(hour)])], axis=1)
                temp_list = [NaN,NaN,NaN,NaN,NaN,NaN]
            current_time = time
        
        #print(time, pollutant, value)
        if (pollutant == "o3"):
            temp_list[0] = value
        elif (pollutant == "pm25"):
            temp_list[1] = value
        elif (pollutant == "pm10"):
            temp_list[2] = value
        elif (pollutant == "co"):
            temp_list[3] = value
        elif (pollutant == "so2"):
            temp_list[4] = value
        elif (pollutant == "no2"):
            temp_list[5] = value

#interpolate (fill in missing values) and reverse the dataframe into ascending early
df = (df.interpolate(axis=1))[df.columns[::-1]]

df.to_csv("test2.csv", mode = 'w', index= False)
# df.to_excel('test.xlsx', sheet_name='sheet1', index=False, appe)

