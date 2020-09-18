import csv
import json
import os
import random
from functools import reduce

import pytest

from src.data_frame import DataFrame
from glob import glob

from src.main import get_csv_file_paths_from_dir


class TestDataFrameUnit:
    @pytest.fixture
    def data_frame(self):
        with open("../configs/students.json") as config_file:
            config = json.load(config_file)
            self.config = config
        datasets_dir = config.get("students_datasets_path")
        datasets_paths = get_csv_file_paths_from_dir(datasets_dir)
        dataset_path = datasets_paths.pop(0)
        self.dataset_path = dataset_path
        with open(self.dataset_path) as dataset_handle:
            reader = csv.reader(dataset_handle)
            headers = next(reader)
            data_frame = DataFrame(headers, reader)
            yield data_frame

    @pytest.fixture
    def small_data_frame(self):
        dataset_path = "../resources/students_test/small.csv"
        self.dataset_path = dataset_path
        with open(self.dataset_path) as dataset_handle:
            reader = csv.reader(dataset_handle)
            headers = next(reader)
            data_frame = DataFrame(headers, reader)
            yield data_frame

    @pytest.fixture
    def small_data_frame2(self):
        dataset_path = "../resources/students_test/small2.csv"
        self.dataset_path = dataset_path
        with open(self.dataset_path) as dataset_handle:
            reader = csv.reader(dataset_handle)
            headers = next(reader)
            data_frame = DataFrame(headers, reader)
            yield data_frame

    def test_sum(self, small_data_frame):
        result = small_data_frame.pluck(11).sum()
        first_row = result.get_row(0)
        headers = result.headers
        assert headers[0] == 'sum'
        assert type(first_row[0]) == float
        assert first_row[0] == 284.6

    def test_avg(self, small_data_frame):
        row = 0
        column = 0
        result = small_data_frame.pluck(11).avg()
        first_row = result.get_row(row)
        headers = result.headers
        assert headers == ['avg']
        assert type(first_row[column]) == float
        assert round(first_row[column]) == 95

    def test_min(self, small_data_frame):
        row = 0
        column = 0
        result = small_data_frame.pluck(11).min()
        first_row = result.get_row(row)
        headers = result.headers
        assert headers == ['min']
        assert type(first_row[column]) == float
        assert first_row[column] == 92

    def test_max(self, small_data_frame):
        row = 0
        column = 0
        result = small_data_frame.pluck(11).max()
        first_row = result.get_row(row)
        headers = result.headers
        assert headers == ['max']
        assert type(first_row[column]) == float
        assert first_row[column] == 97.6

    def test_pluck(self, data_frame):
        result = data_frame.pluck(11)
        first_record = result.get_row(0).pop()
        third_record = result.get_row(2).pop()
        assert int(first_record) == 48
        assert int(third_record) == 91

    def test_filter(self, data_frame):
        result = data_frame.filter(3, 'Iowa').pluck(11).max()
        row = 0
        first_row = result.get_row(row)
        assert result.headers == ['max']
        assert first_row[0] == 100

    def test_filter_again(self, data_frame):
        result = data_frame.filter(3, 'Iowa').pluck(11).max().filter(0, 100)
        row = 0
        first_row = result.get_row(row)
        assert first_row[0] == 100

    def test_ciel(self, small_data_frame):
        result = small_data_frame.pluck(11).ciel()
        third_record = result.get_row(2)[0]
        assert int(third_record) == 98

    def test_merge(self, small_data_frame, small_data_frame2):
        small_data_frame.merge(small_data_frame2)
        small_data_frame.print()

    def test_write_to_csv(self, small_data_frame):
        target_file = "target.csv"
        small_data_frame.write_to_csv(target_file)
        with open(target_file) as target:
            reader = csv.reader(target)
            headers = next(reader)
            first_line = next(reader)
            assert headers == small_data_frame.headers
            assert first_line[0] == 'c5de3580-2694-42fa-a852-26f87c771d6e'


