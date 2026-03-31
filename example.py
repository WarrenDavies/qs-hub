from qshub.main import main
from qshub.utils import utils
from qshub.ingestion import local_files
from qshub.core.storage_manager import StorageManager

from pipelines.registry import pipeline_registry

### Storage
storage = StorageManager()


### INGESTION

ingestion_config_files = utils.get_yaml_files_in_dir("./configs/ingestion/")
ingestion_config_files = [utils.load_yaml(file) for file in ingestion_config_files]

for ingestion_config_file in ingestion_config_files:
    for run in ingestion_config_file["runs"]:
        local_files.collect_local_file(
            run["source_path"],
            run["destination_path"],
        )


### BRONZE TO SILVER

bronze_to_silver_config = utils.load_yaml("./configs/pipelines/bronze-to-silver.yaml")

for run in bronze_to_silver_config["runs"]:
    pipeline = pipeline_registry[run["name"]]
    df = storage.read(run["source_path"])
    df_transformed = pipeline.run(df)
    storage.write(run["destination_path"], df_transformed)