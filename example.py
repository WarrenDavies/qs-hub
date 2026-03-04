from qshub.main import main
from qshub.utils import utils
from qshub.ingestion import local_files


ingestion_config_files = utils.get_yaml_files_in_dir("./configs/ingestion/")
ingestion_config_files = [utils.load_yaml(file) for file in ingestion_config_files]
for ingestion_config_file in ingestion_config_files:
    for run in ingestion_config_file["runs"]:
        local_files.collect_local_file(
            run["source_path"],
            run["destination_path"],
        )

# pipeline_configs = utils.get_yaml_files_in_dir(config_path + "/pipelines")
# pipeline_configs = [utils.yaml_to_dict(file) for file in pipeline_configs]

