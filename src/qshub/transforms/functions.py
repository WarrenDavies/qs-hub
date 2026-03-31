import pandas as pd


def rename_cols(df, col_name_map):
    df_renamed = df.copy()
    df_renamed = df_renamed.rename(columns=col_name_map)

    return df_renamed


