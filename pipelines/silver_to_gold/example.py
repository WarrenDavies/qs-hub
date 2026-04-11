from qshub.core.silver_to_gold_pipeline import SilverToGoldPipeline
from qshub.transforms.transform import Transform
from qshub.transforms import functions as tf


example_to_gold_events = SilverToGoldPipeline([
    Transform(tf.drop_cols, {"cols_to_drop": ["row_hash", "ingest_ts", "source_file"]}, "drop_cols")
])


example_to_gold_daily = SilverToGoldPipeline([
    Transform(tf.drop_cols, {"cols_to_drop": ["row_hash", "ingest_ts", "source_file"]}, "drop_cols"),
    Transform(tf.group_and_average, {"grouping_cols": ["date", "metric"], "agg_cols": ["metric_value"]}, "group_and_average"),
])