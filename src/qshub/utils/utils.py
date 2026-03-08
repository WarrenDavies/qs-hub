import yaml
import os
import datetime

from pathlib import Path


def load_yaml(path):
    path = Path(path)
    with open(str(path), 'r') as file:
        loaded_yaml = yaml.safe_load(file)
    return loaded_yaml


def get_yaml_files_in_dir(path = "./configs/pipelines"):
    path = Path(path)
    yaml_files = list(path.glob('*.yaml')) + list(path.glob('*.yml'))
    return yaml_files


def get_ts():
    ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    return ts