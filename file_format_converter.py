import glob
import json
import os
import re

import pandas as pd


def get_column_names(schemas, data_set_name, sorting_key="column_position"):
    column_details = schemas[data_set_name]
    columns = sorted(column_details, key=lambda column: column[sorting_key])
    return [col['column_name'] for col in columns]


def read_csv(file_name, schemas):
    file_path_splits = re.split('[/\\\]', file_name)
    dataset_name = file_path_splits[-2]
    target_file_name = file_path_splits[-1]
    columns = get_column_names(schemas, dataset_name)
    df = pd.read_csv(file_name, names=columns)
    return df, target_file_name


def to_json(df, target_base_dir, dataset_name, target_file_name):
    target_json_file_path = f"{target_base_dir}/{dataset_name}/{target_file_name}"
    target_json_folder = f"{target_base_dir}/{dataset_name}"
    os.makedirs(target_json_folder, exist_ok=True)
    df.to_json(target_json_file_path, orient="records", lines=True)


def file_converter(src_base_dir, target_base_dir, dataset_name):
    schemas = json.load(open(f"{src_base_dir}/schemas.json"))
    src_file_names = glob.glob(f"{src_base_dir}/{dataset_name}/part-*")
    for file_name in src_file_names:
        df, target_file_name = read_csv(file_name, schemas)
        to_json(df, target_base_dir, dataset_name, target_file_name)


def process_files(dataset_names=None):
    src_base_dir = "data/retail_db"
    target_base_dir = "data/retail_db_json"
    schemas = json.load(open(f"{src_base_dir}/schemas.json"))
    if not dataset_names:
        dataset_names = schemas.keys()
    for dataset_name in dataset_names:
        print(f"Processing dataset '{dataset_name}'")
        file_converter(src_base_dir, target_base_dir, dataset_name)


process_files()
