import pandas as pd

from abc import ABC, abstractmethod
from typing import List, Dict
from matplotlib import pyplot as plt
from matplotlib.pyplot import figure

from src.generic_functions import get_df_sub_from_df_mega
from src.clean_data import CleanDataHelper


class AbstractAnalyticClass(ABC):
    @abstractmethod
    def get_df_sub_from_df_mega(self, metric_name: str):
        pass

    @abstractmethod
    def get_firm_desc(self, list_of_df: List):
        pass

    @abstractmethod
    def run(self):
        pass


class GenericAnalyticClass(AbstractAnalyticClass):
    CLASS_METRIC: str
    WEIGHT = "weight"
    ORDER_NO = "order_no"
    ORDER_NO_WEIGHTED = "order_no_weighted"

    def __init__(self, df_mega: pd.DataFrame, firm_metrics: Dict, output_folder: str):
        self.df_mega = df_mega
        self.firm_metrics = firm_metrics
        self.output_folder = output_folder

    def run(self):
        raise NotImplementedError("method must be implemented from child class")

    def save_file_full_path(self, save_file_name: str = ""):
        return self.output_folder + "/" + self.__class__.__name__ + "_" + save_file_name + ".png"

    def get_df_sub_from_df_mega(self, metric_name: str):
        metric_weight, clean_flag = self.firm_metrics[metric_name]

        df_sub = get_df_sub_from_df_mega(df_mega=self.df_mega, like_filter=metric_name).copy()

        if clean_flag:
            df_sub = CleanDataHelper.run(df_sub=df_sub)
        else:
            df_sub[CleanDataHelper.INDICATOR] = df_sub.apply(
                lambda row: CleanDataHelper.indicate_missing_data(row=row),
                axis=1,
            )
            df_sub = df_sub[df_sub[CleanDataHelper.INDICATOR] > 0]

        _df_sub = df_sub.copy()
        _df_sub[self.WEIGHT] = metric_weight
        _df_sub.name = metric_name

        return _df_sub

    def get_firm_desc(self, list_of_df: List):
        ds = list_of_df[0][self.ORDER_NO_WEIGHTED]

        if len(list_of_df) > 1:
            for _ds in list_of_df[1:]:
                ds = ds.add(_ds[self.ORDER_NO_WEIGHTED])

            ds = ds.dropna()
            ds.sort_values(ascending=False, inplace=True)

        return ds.index.to_list()


class ProcessSeparateAnalyticClass(GenericAnalyticClass):
    @abstractmethod
    def get_df_processed(self, df: pd.DataFrame, common_indexes: List):
        pass

    def save_plot(self, list_of_df: List):
        _list_of_df = []

        for df in list_of_df:
            name = df.name + "_" + self.CLASS_METRIC
            df_temp = df[[self.CLASS_METRIC]]
            df_temp = df_temp.rename(columns={self.CLASS_METRIC: name})
            _list_of_df.append(df_temp)

        df = pd.concat(_list_of_df, axis=1)

        df = df.head(15)
        plt.rcParams["figure.figsize"] = (22, 15)
        df.plot(kind='bar', alpha=0.75, rot=30)

        plt.savefig(self.save_file_full_path())

    def run(self) -> List:
        list_of_df = []
        list_of_df_proc = []

        for key, value in self.firm_metrics.items():
            list_of_df.append(self.get_df_sub_from_df_mega(metric_name=key))

        common_indexes = list_of_df[0].index
        for _df_temp in list_of_df[1:]:
            common_indexes = _df_temp.index.intersection(common_indexes)

        for df in list_of_df:
            list_of_df_proc.append(self.get_df_processed(df=df, common_indexes=common_indexes))

        self.save_plot(list_of_df=list_of_df_proc)

        firm_desc_list = self.get_firm_desc(list_of_df=list_of_df_proc)

        return firm_desc_list

