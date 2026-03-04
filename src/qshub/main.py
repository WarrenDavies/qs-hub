import yaml
import os

from dotenv import load_dotenv

from qshub.utils import utils


def main():

    ingestion_config_files = utils.get_yaml_files_in_dir("./configs/ingestion/")
    ingestion_config_files = [utils.load_yaml(file) for file in ingestion_config_files]
    for ingestion_config_file in ingestion_config_files:
        for run in ingestion_config_file["runs"]:
            local_files.collect_local_file(
                run["source_path"],
                run["destination_path"],
            )

if __name__ == "__main__":
    main()