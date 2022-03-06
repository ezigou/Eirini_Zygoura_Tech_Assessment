import pandas as pd
import numpy as np
from typing import List

from src.clean_data import CleanDataHelper
from src.analytics.class_generic import ProcessSeparateAnalyticClass


class FirmSizeAnalyticClass(ProcessSeparateAnalyticClass):
    CLASS_METRIC = "average"

    @classmethod
    def sort_df_based_on_class(cls, df_sub: pd.DataFrame):
        _df_sub = df_sub.copy()
        _df_sub[cls.CLASS_METRIC] = _df_sub.sum(axis=1) / _df_sub[CleanDataHelper.INDICATOR]
        _df_sub.sort_values(by=[cls.CLASS_METRIC], inplace=True, ascending=False)
        _df_sub[cls.ORDER_NO] = np.arange(_df_sub.shape[0])

        return _df_sub

    def get_df_processed(self, df: pd.DataFrame, common_indexes: List):
        name = df.name

        df_sub = df[df.index.isin(common_indexes)]

        _df_sub = df_sub.copy()
        _df_sub = self.sort_df_based_on_class(df_sub=_df_sub)
        _df_sub[self.ORDER_NO_WEIGHTED] = _df_sub[self.ORDER_NO] * _df_sub[self.WEIGHT]

        _df_sub.name = name

        return _df_sub
