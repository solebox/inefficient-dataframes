import json
import os
from glob import glob


def get_csv_file_paths_from_dir(dir_path):
    return glob(os.path.join(dir_path, '*.csv'))


def get_file_locations():
    config_path = "../configs/students.json"
    with open(config_path) as config_file:
        config = json.load(config_file)
        path_to_csvs = config.get("students_datasets_path")
        file_loctions = get_csv_file_paths_from_dir(path_to_csvs)
        return file_loctions


def parallel_play():
    print("lets do this in parallel")


def flatten():
    print("lets flatten it")


if __name__ == "__main__":
    print(get_file_locations())