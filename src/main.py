import os
from concurrent.futures.thread import ThreadPoolExecutor

from file_helpers import get_file_locations, read_dataframe_from_csv


def transformation(source_path, target_path):
    print(f"transforming {source_path} to {target_path}\n")
    data_frame = read_dataframe_from_csv(source_path)
    data_frame.filter(3, 'Iowa').pluck(11).max().write_to_csv(target_path)


def parallel_play():
    file_locations = get_file_locations()
    file_targets = []
    results = []
    with ThreadPoolExecutor(max_workers=6) as executor:
        for file_location in file_locations:
            target = os.path.join("../resources/output", os.path.basename(file_location))
            file_targets.append(target)
            future = executor.submit(transformation, file_location, target)
            results.append(future)


def flatten():
    file_locations = get_file_locations()
    target = os.path.join("../resources/output", "flattened.csv")
    first_file = file_locations.pop(0)
    data_frame = read_dataframe_from_csv(first_file)
    data_frame = data_frame.pluck(11).avg()
    for file_location in file_locations:
        data_frame2 = read_dataframe_from_csv(file_location)
        data_frame.merge(data_frame2.pluck(11).avg())
    data_frame.max().write_to_csv(target)


if __name__ == "__main__":
    parallel_play()
    flatten()