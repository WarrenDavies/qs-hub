import pandas as pd
import hashlib

def rename_cols(df, col_name_map):
    df_renamed = df.copy()
    df_renamed = df_renamed.rename(columns=col_name_map)

    return df_renamed


def hash_row(row):
    return hashlib.sha256(
        "|".join(map(str, row.values)).encode()
    ).hexdigest()


def add_hash_col(df):
    df["row_hash"] = df.apply(hash_row, axis=1)
    return df