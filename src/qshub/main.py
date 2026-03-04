import yaml
import os

from dotenv import load_dotenv

from qshub.utils import utils


def main():

    load_dotenv()
    config_path = os.getenv('config_path')

    pipeline_configs = utils.get_yaml_files_in_dir(config_path + "/pipelines")
    pipeline_configs = [utils.yaml_to_dict(file) for file in pipeline_configs]

if __name__ == "__main__":
    main()