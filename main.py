import os
import pandas as pd
import datetime
from pathlib import Path

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
    processed_log_path = Path(run["source_path"]).parent / "processed_log.csv"
    df_processed_log = storage.read(processed_log_path)
    files_to_process = storage.get_files_to_process_by_filename(run["source_path"], df_processed_log)

    for file in files_to_process:
        print("processing", file)
        ingest_ts = datetime.datetime.now().isoformat()
        
        if os.path.getsize(file) == 0:
            print("   File is empty...skipping")
            continue

        checksum = storage.get_checksum(file)
        if storage.checksum_exists(checksum, df_processed_log):
            print("   Checksum exists...skipping")
            continue
        print("   IMPORTING")
        pipeline = pipeline_registry[run["name"]]
        df_input = storage.read(file)
        df_input = tf.add_hash_col(df_input)
        
        if storage.exists(run["destination_path"]):
            df_silver_data = storage.read(run["destination_path"])
            df_silver_latest = (
                df_silver_data
                .sort_values("ingest_ts")
                .drop_duplicates(subset=[config["row_id_col_name"]], keep="last")
            )
            df_new_rows = pipeline.get_new_rows(df_input, df_silver_latest)
            df_new_rows = pipeline.run(df_new_rows)
            df_new_rows = pd.concat([df_silver_data, df_new_rows], ignore_index=True)
        else:
            df_new_rows = pipeline.run(df_input)
        df_new_rows["ingest_ts"] = ingest_ts
        df_new_rows["source_file"] = file       
        storage.write(run["destination_path"], df_new_rows)

        new_processing_log = {
            "ingest_ts": ingest_ts,
            "filename": Path(file).name,
            "checksum": checksum 
        }
        df_processed_log = pd.concat([df_processed_log, pd.DataFrame([new_processing_log])], ignore_index=True)
        storage.write(processed_log_path, df_processed_log)


### SILVER TO GOLD
silver_to_gold_config = utils.load_yaml("./configs/pipelines/silver-to-gold.yaml")

for run in silver_to_gold_config["runs"]:
    print("processing", run)
    
    pipeline = pipeline_registry[run["name"]]
    df_silver = storage.read(run["source_path"])
    df_silver = pipeline.get_latest_rows(df_silver, key_cols=["date"], sort_col="ingest_ts")
    df_gold = pipeline.run(df_silver)
   
    storage.write(run["destination_path"], df_gold)
