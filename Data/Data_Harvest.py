from numpy import NaN
import requests
from pandas import DataFrame, date_range
from datetime import datetime, timedelta
from meteostat import Hourly
import pandas as pd



def data_collect(location, time, list, dataframe):
    data = Hourly(location, datetime.fromisoformat(str(time).split('+')[0]), datetime.fromisoformat(str(time).split('+')[0]))
    data = data.fetch()
    if data.empty:
        list.extend([NaN, NaN, NaN])
    else: list.extend([data.iloc[0]["temp"], data.iloc[0]["rhum"], data.iloc[0]["wspd"]])
    return list



def data_harvest(city, openaqlocal, weatherstationlocal, filename):
    df = DataFrame(columns=["City", "Time", "O3", "PM2.5", "PM10", "CO", "SO2", "NO2", "Temp", "RHum", "WSpd"]).astype({"City": str, "Time": str, "O3": float, "PM2.5": float, "PM10": float, "CO": float, "SO2": float, "NO2": float, "Temp":float, "RHum":float, "WSpd":float})
    #df[["PM2.5", "PM10", "NO2", "CO", "SO2", "O3", "Temp", "RHum", "WSpd", "O3 P1", "O3 P4", "O3 P8", "O3 P24",  "PM2.5 P1", "PM2.5 P4", "PM2.5 P8", "PM2.5 P24"]] = df[["PM2.5", "PM10", "NO2", "CO", "SO2", "O3", "Temp", "RHum", "WSpd", "O3 P1", "O3 P4", "O3 P8", "O3 P24",  "PM2.5 P1", "PM2.5 P4", "PM2.5 P8", "PM2.5 P24"]].apply(pd.to_numeric)

    urltemplates = ["https://api.openaq.org/v2/measurements?date_from=2022-01-01T01%3A00%3A00%2B00%3A00&date_to=2022-03-31T01%3A50%3A00%2B00%3A00&limit=100000&page=1&offset=0&sort=desc&parameter=pm10&parameter=pm25&parameter=so2&parameter=o3&parameter=co&parameter=no2&radius=1000&order_by=datetime&isMobile=false&sensorpollutant=reference%20grade&location_id={openaq}".format(openaq = openaqlocal),
    "https://api.openaq.org/v2/measurements?date_from=2021-10-01T01%3A00%3A00%2B00%3A00&date_to=2022-01-01T01%3A50%3A00%2B00%3A00&limit=100000&page=1&offset=0&sort=desc&parameter=pm10&parameter=pm25&parameter=so2&parameter=o3&parameter=co&parameter=no2&radius=1000&order_by=datetime&isMobile=false&sensorpollutant=reference%20grade&location_id={openaq}".format(openaq = openaqlocal),
    "https://api.openaq.org/v2/measurements?date_from=2021-07-01T01%3A00%3A00%2B00%3A00&date_to=2021-09-30T01%3A50%3A00%2B00%3A00&limit=100000&page=1&offset=0&sort=desc&parameter=pm10&parameter=pm25&parameter=so2&parameter=o3&parameter=co&parameter=no2&radius=1000&order_by=datetime&isMobile=false&sensorpollutant=reference%20grade&location_id={openaq}".format(openaq = openaqlocal),
    "https://api.openaq.org/v2/measurements?date_from=2021-04-01T01%3A00%3A00%2B00%3A00&date_to=2021-06-30T01%3A50%3A00%2B00%3A00&limit=100000&page=1&offset=0&sort=desc&parameter=pm10&parameter=pm25&parameter=so2&parameter=o3&parameter=co&parameter=no2&radius=1000&order_by=datetime&isMobile=false&sensorpollutant=reference%20grade&location_id={openaq}".format(openaq = openaqlocal)]

    #must split into 4 URLs to not overdraw
    for url in urltemplates:
        headers = {"Accept": "application/json"}

        response = requests.get(url, headers=headers)
        print(response.text)
        datapoints = response.text.split("},{")
        temp_list = [NaN,NaN,NaN,NaN,NaN,NaN,NaN,NaN]
        current_time = datapoints[0][datapoints[0].index("\"utc\":\"") + len("\"utc\":\""):datapoints[0].index("\",\"local")]
        
        for elem in datapoints:
            time = elem[elem.index("\"utc\":\"") + len("\"utc\":\""):elem.index("\",\"local")]
            pollutant = elem[elem.index("parameter\":\"") + len("parameter\":\""):elem.index("\",\"value")]
            value = float(elem[elem.index("value\":") + len("value\":"):elem.index(",\"date\"")])
            print(time)
            if (datetime.fromisoformat(time) == datetime.fromisoformat(current_time) - timedelta(hours=1)):
                temp_list[0], temp_list[1] = city, str(datetime.fromisoformat(current_time))
                temp_list = data_collect(location=weatherstationlocal, time = current_time, list = temp_list, dataframe = df)            
                #df = pd.concat([df, pd.DataFrame(temp_list, columns = [str(datetime.fromisoformat(current_time))])], axis=0)
                # df = pd.concat((df, pd.DataFrame([temp_list])), axis=1)
                df.loc[len(df)] = temp_list
                temp_list = [NaN,NaN,NaN,NaN,NaN,NaN,NaN,NaN]
                current_time = time

            #happens when data is missing for hour(s)
            elif (time != current_time):
                temp_list[0], temp_list[1] = city, str(datetime.fromisoformat(current_time))
                temp_list = data_collect(location = weatherstationlocal, time = current_time, list = temp_list, dataframe = df)
                df.loc[len(df)] = temp_list
                temp_list = [NaN,NaN,NaN,NaN,NaN,NaN,NaN,NaN]

                time_range = date_range(time, current_time, freq="H", inclusive = "neither")
                for hour in time_range:
                    print(hour)
                    data_collect(location=weatherstationlocal, time= str(hour), list = temp_list, dataframe = df)        
                    #df = pd.concat([df, pd.DataFrame(temp_list, columns = [str(hour)])], axis=1)
                    temp_list[0], temp_list[1] = city, str(hour)
                    df.loc[len(df)] = temp_list
                    temp_list = [NaN,NaN,NaN,NaN,NaN,NaN,NaN,NaN]
                current_time = time
            
            if (pollutant == "o3"):
                temp_list[2] = value
            elif (pollutant == "pm25"):
                temp_list[3] = value
            elif (pollutant == "pm10"):
                temp_list[4] = value
            elif (pollutant == "co"):
                temp_list[5] = value
            elif (pollutant == "so2"):
                temp_list[6] = value
            elif (pollutant == "no2"):
                temp_list[7] = value
        print("Quarter Complete")

    #interpolate (fill in missing values) and reverse the dataframe into ascending early
    df = (df.interpolate(axis=0)).loc[::-1]

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


    df.to_csv(filename, mode = 'w', index= False)


data_harvest(city ="Denver", openaqlocal="1265", weatherstationlocal="KBKF0", filename="Denver2021.csv")
#data_harvest(city = "Bangkok", openaqlocal="2352", weatherstationlocal="48455", filename="Bangkok2021.csv")
# data_harvest(openaqlocal="1265", weatherstationlocal="KBKF0", filename="Denver2019.csv")
# data_harvest(openaqlocal="1265", weatherstationlocal="KBKF0", filename="Denver2019.csv")
# data_harvest(openaqlocal="1265", weatherstationlocal="KBKF0", filename="Denver2019.csv")
