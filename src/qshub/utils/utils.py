import yaml
import os
import hashlib

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


def get_file_hash(path):
    hasher = hashlib.sha256()

    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            hasher.update(chunk)

    return hasher.hexdigest()