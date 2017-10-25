from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.protocol import JSONValueProtocol
import re
import itertools
import csv

class UsersCount(MRJob):
    def mapper_flights(self, _, line):
        flight_data = line.split(",")
        yield flight_data[1], 1

    def reducer1(self, month, frecuency):
        count = len([m for m in frecuency])
        yield 'MAX', tuple([month, count])

    def reducer2(self, tag, data):
        data_list = [d for d in data]
        max_value = 0
        max_month = ""
        for d in data_list:
            if d[1] > max_value:
                max_month = d[0]
                max_value = d[1]
        yield max_month, max_value

    def steps(self):
        return [MRStep(mapper=self.mapper_flights, reducer=self.reducer1),
                MRStep(reducer=self.reducer2)]

if __name__ == '__main__':
    UsersCount.run()
