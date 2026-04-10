from qshub.core.silver_to_gold_pipeline import SilverToGoldPipeline
from qshub.transforms.transform import Transform
from qshub.transforms import functions as tf



example_to_gold_events = SilverToGoldPipeline([
    Transform(tf.drop_cols, ["row_hash", "ingest_ts", "source_file"])
])