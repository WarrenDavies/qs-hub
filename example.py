import pandas as pd

from qshub.main import main
from qshub.utils import utils
from qshub.ingestion import local_files
from qshub.core.storage_manager import StorageManager
from qshub.transforms import functions as tf

from pipelines.registry import pipeline_registry

### STORAGE
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
    
    df_transformed = tf.add_hash_col(df_transformed)
    if storage.exists(run["destination_path"]):
        df_silver_data = storage.read(run["destination_path"])
        df_new_rows = pipeline.get_new_rows(df_transformed, df_silver_data)
        df_new_rows = pd.concat([df_silver_data, df_new_rows])
    else:
        df_new_rows = df_transformed

    storage.write(run["destination_path"], df_new_rows)