from qshub.core.bronze_to_silver_pipeline import BronzeToSilverPipeline
from qshub.transforms.transform import Transform
from qshub.transforms import functions as tf

COL_NAME_MAP = {
    "DATE": "date"
}

example_pipeline = BronzeToSilverPipeline([
    Transform(tf.rename_cols, {"col_name_map": COL_NAME_MAP}, "rename_cols")
])