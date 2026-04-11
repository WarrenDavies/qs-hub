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


def group_and_sum(df_input, grouping_cols, agg_cols):
    df_output = df_input.copy()
    df_output = df_output[grouping_cols + agg_cols]
    df_output = df_output.groupby(grouping_cols).sum()
    df_output = df_output.reset_index()
    return df_output


def group_and_average(df_input, grouping_cols, agg_cols):
    df_output = df_input.copy()
    df_output = df_output[grouping_cols + agg_cols]
    df_output = df_output.groupby(grouping_cols).mean()
    df_output = df_output.reset_index()
    return df_output


def to_datetime(df_input, datetime_col):
    df_output = df_input.copy()
    df_output[datetime_col] = pd.to_datetime(df_output[datetime_col])
    return df_output


def split_datetime(df_input, datetime_col, drop_datetime_col=False, bring_to_left=True):
    df_output = df_input.copy()
    df_output['date'] = df_output[datetime_col].dt.date
    df_output['time'] = df_output[datetime_col].dt.time
    if drop_datetime_col:
        df_output = df_output.drop(columns=[datetime_col])
    if bring_to_left:
        cols = list(df_output.columns.values)
        cols.remove("date")
        cols.remove("time")
        cols = ["date", "time"] + cols
        df_output = df_output[cols]
    return df_output