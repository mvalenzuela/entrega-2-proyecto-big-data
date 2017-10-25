from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.protocol import JSONValueProtocol
import re
import itertools
import csv

class UsersCount(MRJob):
    def mapper_flights(self, _, line):
        flight_data = line.split(",")
        yield flight_data[1], flight_data[25]

    def reducer1(self, month, weather_delays):
        lista_delays = [int(wd) for wd in weather_delays if wd!= "WeatherDelay" and wd != "NA"]
        if len(lista_delays) != 0:
            yield month, sum(lista_delays)*1.0/len(lista_delays)

    def steps(self):
        return [MRStep(mapper=self.mapper_flights, reducer=self.reducer1)]

if __name__ == '__main__':
    UsersCount.run()
