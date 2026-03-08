from itertools import chain

from qshub.main import main
from qshub.utils import utils
import qshub.ingestion.functions.registry as func_registry


class IngestionManager():


    def __init__(self, config_path = "./configs/ingestion"):
        self.config_path = config_path
        self.config = self.combine_configs()


    def combine_configs(self):
        config_file_paths = utils.get_yaml_files_in_dir(self.config_path)
        configs = [utils.load_yaml(file) for file in config_file_paths]
        configs = list(chain.from_iterable(configs))

        return configs

    
    def ingest_all(self):
        for data_source in self.config:
            ingestion_func = func_registry.get_func(data_source["function_name"])
            ingestion_func(**data_source["params"])