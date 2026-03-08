import shutil
from pathlib import Path

from qshub.utils import utils
from qshub.ingestion.functions.registry import register





@register("ingest_local_file")
def ingest_local_file(source_path, destination_path):
    source_path = Path(source_path)
    destination_path = Path(destination_path)
    new_filename = f"{source_path.stem}_{utils.get_ts()}{source_path.suffix}"
    destination_path = destination_path / new_filename
    destination_path.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(source_path, destination_path)
    