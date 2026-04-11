import pandas as pd

from qshub.core.pipeline import BasePipeline
from qshub.transforms import functions as tf


class SilverToGoldPipeline(BasePipeline):

    def __init__(self, steps):
        super().__init__(steps)


    def get_new_files():
        pass


    @staticmethod
    def get_latest_rows(df_silver_data, key_cols, sort_col="ingest_ts"):
        df_silver_latest = (
            df_silver_data
            .sort_values(sort_col)
            .drop_duplicates(subset=key_cols, keep="last")
        )

        return df_silver_latest