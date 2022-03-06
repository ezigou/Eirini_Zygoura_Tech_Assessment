import pandas as pd


def get_df_sub_from_df_mega(df_mega: pd.DataFrame, like_filter) -> pd.DataFrame:
    df_temp = df_mega.filter(like=like_filter)

    df_temp.columns = df_temp.columns.droplevel(level=0)

    return df_temp
