class DataPoint:
  def __init__(self, time, pollutants, weather):
    self.time = time
    self.pm25 = pollutants[0]
    self.pm10 = pollutants[1]
    self.no2 = pollutants[2]
    self.co = pollutants[3]
    self.so2 = pollutants[4]
    self.o3 = pollutants[5]
