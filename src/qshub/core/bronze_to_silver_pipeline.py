import pandas as pd

from qshub.core.pipeline import BasePipeline
from qshub.transforms import functions as tf


class BronzeToSilverPipeline(BasePipeline):

    def __init__(self, steps):
        super().__init__(steps)


    def get_new_files():
        pass


    @staticmethod
    def get_new_rows(df_bronze_data, df_silver_data):
        df_merged = pd.merge(df_bronze_data, df_silver_data[["row_hash"]], on="row_hash", how="outer", indicator=True)

        df_new_rows = df_merged[df_merged["_merge"] == "left_only"]
        df_new_rows = df_new_rows.drop(columns=['_merge'])

        return df_new_rows