import pandas as pd
from typing import List
from sklearn.neighbors import LocalOutlierFactor
from mpl_toolkits.mplot3d import Axes3D
from matplotlib import pyplot as plt

from src.clean_data import CleanDataHelper
from src.analytics.class_generic import GenericAnalyticClass


class OutliersAnalyticClass(GenericAnalyticClass):
    OUTLIERS = "outliers"

    def create_3d_scatter_plot(self, x1, x2, x3, output, save_file_name: str):
        # Create 3D Scatter Plot of dominant features
        plot = Axes3D(plt.figure(1), elev=-150, azim=100)
        plot.scatter(
            x1,
            x2,
            x3,
            c=output,
            cmap=plt.cm.bwr
        )
        plot.set_title("3D Plot of Outliers")
        plot.set_xlabel("GWP")
        plot.set_ylabel("NWP")
        plot.set_zlabel("SCR COVERAGE RATIO")
        save_file_path = self.save_file_full_path(save_file_name=save_file_name)
        plt.savefig(save_file_path)

    def add_outliers(self, list_of_df: List, columns_to_select: List):
        df = pd.concat(list_of_df, axis=1)

        for col in columns_to_select:
            df_sub = df[[col]]
            df[self.OUTLIERS + "_" + col] = LocalOutlierFactor().fit_predict(df_sub.values)

        columns_to_keep = list(set(df.columns) - set(columns_to_select))

        df_outliers = df[columns_to_keep].copy()
        df_outliers[self.ORDER_NO_WEIGHTED] = df_outliers.sum(axis=1)
        df_outliers = df_outliers.sort_values(by=[self.ORDER_NO_WEIGHTED])

        df_concat = pd.concat([df, df_outliers[self.ORDER_NO_WEIGHTED]], axis=1)

        return df_concat

    def get_df_processed(self, list_of_df: List, common_indexes: List):
        _list_of_df = []
        columns_to_select = list_of_df[0].loc[
                   :,
                   (
                           (list_of_df[0].columns != self.WEIGHT)
                           &
                           (list_of_df[0].columns != CleanDataHelper.INDICATOR)
                   )
                   ].columns

        for _df in list_of_df:
            df_temp = _df[_df.index.isin(common_indexes)][columns_to_select].copy()
            _list_of_df.append(df_temp)

        df_outliers = self.add_outliers(list_of_df=_list_of_df, columns_to_select=columns_to_select)

        return df_outliers

    def run(self):
        list_of_df = []

        for key, value in self.firm_metrics.items():
            list_of_df.append(self.get_df_sub_from_df_mega(metric_name=key))

        common_indexes = list_of_df[0].index.intersection(list_of_df[1].index)

        if len(list_of_df) > 2:
            for _df_temp in list_of_df[2:]:
                common_indexes = _df_temp.index.intersection(common_indexes)

        df_outliers = self.get_df_processed(list_of_df=list_of_df, common_indexes=common_indexes)

        for col in ["2016", "2017", "2018", "2019", "2020"]:
            df_sub = df_outliers[[col, self.OUTLIERS + "_" + col]]
            df_sub.columns = [1, 2, 3, 4]

            self.create_3d_scatter_plot(
                x1=df_sub[1],
                x2=df_sub[2],
                x3=df_sub[3],
                output=df_sub[4],
                save_file_name=col
            )

        outliers_list = self.get_firm_desc([df_outliers])

        return outliers_list
