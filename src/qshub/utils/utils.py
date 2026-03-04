import yaml
import os

from pathlib import Path


def yaml_to_dict(path):
    path = Path(path)
    with path.open('r') as stream:
        dict_ = yaml.safe_load(stream)
    return dict_


def get_yaml_files_in_dir(path = "./configs/pipelines"):
    path = Path(path)
    yaml_files = list(path.glob('*.yaml')) + list(path.glob('*.yml'))
    return yaml_files
