import csv
import math
from functools import reduce
from itertools import tee, chain
from typing import Iterable


class DataFrame:

    def __init__(self, headers: list, data: Iterable[list]):
        self._headers = headers
        self._data = self._get_data_generator(data)

    def _get_first_column_numeric_data(self, data):
        # since all the number specific methods take the first column and ignore non numeric
        # keeping it dry
        data = self._get_data_generator(data)
        for row in data:
            try:
                yield float(row[0])
            except Exception:
                yield [0]
    @property
    def headers(self):
        return self._headers

    def _get_data_generator(self, data):
        self._data, iter = tee(data) #fixme - apperantly this evil copier iterates over the data and loads the unused iterator into mem
        return iter

    def _pluck(self, position):
        data = self._get_data_generator(self._data)
        for row in data:
            yield [row[position-1]]

    def pluck(self, position):
        headers = [self._headers[position-1]]
        result_data = self._pluck(position)
        result = self.__class__(headers, result_data)
        return result

    def print(self):
        headers = self._headers
        data = self._get_data_generator(self._data)
        print(headers)
        for row in data:
            print(row)

    def get_row(self, row_number):
        data = self._get_data_generator(self._data)
        row = None
        for index in range(row_number+1):
            row = next(data)
        return row

    def sum(self):
        data = self._get_first_column_numeric_data(self._data)
        headers = ['sum']
        result = reduce(lambda a, b: a + b, data)
        data = [[result]]
        return self.__class__(headers, data)

    def avg(self):
        data = self._get_first_column_numeric_data(self._data)
        headers = ['avg']
        summation = 0
        count = 0
        result = 0
        for row in data:
            count += 1
            summation += row
        if count != 0:
            result = summation/count
        data = [[result]]
        return self.__class__(headers, data)

    def min(self):
        data = self._get_first_column_numeric_data(self._data)
        headers = ['min']
        result = reduce(lambda a, b: a if a < b else b, data)
        data = [[result]]
        return self.__class__(headers, data)

    def max(self):
        data = self._get_first_column_numeric_data(self._data)
        headers = ['max']
        result = reduce(lambda a, b: a if a > b else b, data)
        data = [[result]]
        return self.__class__(headers, data)

    def ciel(self):
        data = self._get_first_column_numeric_data(self._data)
        headers = [self._headers[0]]
        result = map(lambda x: [math.ceil(x)], data)
        return self.__class__(headers, result)

    def _filter(self, column_index, value):
        data = self._get_data_generator(self._data)
        for row in data:
            if row[column_index] == value:
                yield row

    def filter(self, column_index, value):
        headers = self._headers
        data = self._filter(column_index, value)
        return self.__class__(headers, data)

    def merge(self, *dataframes):
        for dataframe in dataframes:
            self._data = chain(self._data, self._get_data_generator(dataframe._data))
        return self.__class__(self._headers, self._data)

    def write_to_csv(self, target):
        data = self._get_data_generator(self._data)
        with open(target, 'w') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(self._headers)
            for row in data:
                writer.writerow(row)
        return self.__class__(self._headers, self._data)
