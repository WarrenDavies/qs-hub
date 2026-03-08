import shutil
from pathlib import Path
import datetime


def get_ts():
    ts = datetime.datetime.now().strftime("%Y%m%d_%H%M")
    return ts


def ingest_local_file(source_path, destination_path):
    source_path = Path(source_path)
    destination_path = Path(destination_path)
    new_filename = f"{source_path.stem}_{get_ts()}{source_path.suffix}"
    destination_path = destination_path / new_filename
    destination_path.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(source_path, destination_path)
    