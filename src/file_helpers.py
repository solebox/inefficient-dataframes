import csv
import json
import os
from glob import glob

from data_frame import DataFrame


def get_csv_file_paths_from_dir(dir_path):
    return glob(os.path.join(dir_path, '*.csv'))


def get_file_locations():
    config_path = "../configs/students.json"
    with open(config_path) as config_file:
        config = json.load(config_file)
        path_to_csvs = config.get("students_datasets_path")
        file_loctions = get_csv_file_paths_from_dir(path_to_csvs)
        return file_loctions


def read_dataframe_from_csv(path):
    # fixme - need to close file descriptor.
    dataset_handle = open(path)
    reader = csv.reader(dataset_handle)
    headers = next(reader)
    data_frame = DataFrame(headers, reader)
    return data_frame