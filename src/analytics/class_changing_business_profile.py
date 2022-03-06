import pandas as pd
import numpy as np

from sklearn.preprocessing import Normalizer

from src.clean_data import CleanDataHelper
from src.analytics.class_generic import ProcessSeparateAnalyticClass


class ChangingBusinessProfileAnalyticClass(ProcessSeparateAnalyticClass):
    CLASS_METRIC = "mean_absolute_deviation"

    def get_mean_absolute_deviation(self, df_sub: pd.DataFrame):
        df_filtered = df_sub.loc[
                      :,
                      (
                              (df_sub.columns != self.WEIGHT)
                              &
                              (df_sub.columns != CleanDataHelper.INDICATOR)
                      )
                      ].copy()

        # normalize data
        df_filtered.iloc[:, :] = Normalizer(norm='max').fit_transform(df_filtered)
        # get the mean absolute deviation
        df_filtered[self.CLASS_METRIC] = df_filtered.mad(axis=1, skipna=True)

        df_concat = pd.concat([df_filtered, df_sub[self.WEIGHT]], axis=1)

        return df_concat

    def get_df_processed(self, df: pd.DataFrame, common_indexes):
        name = df.name

        df_sub = df[df.index.isin(common_indexes)]
        df_sub = self.get_mean_absolute_deviation(df_sub=df_sub)

        _df_sub = df_sub.copy()
        _df_sub.sort_values(by=[self.CLASS_METRIC], inplace=True, ascending=False)
        _df_sub[self.ORDER_NO] = np.arange(_df_sub.shape[0])
        _df_sub[self.ORDER_NO_WEIGHTED] = _df_sub[self.ORDER_NO] * _df_sub[self.WEIGHT]
        _df_sub.name = name

        return _df_sub
