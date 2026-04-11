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


def drop_cols(df_input, cols_to_drop):
    df_output = df_input.copy()
    df_output = df_output.drop(columns=cols_to_drop)

    return df_output  