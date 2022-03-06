from typing import List, Dict
import pandas as pd


class SplitDFToCSVFiles(object):
    def __init__(self, config: Dict, src_file: str, output_folder: str):
        self.src_file = src_file
        self.output_folder = output_folder
        self.sheet_details = config["sheet_details"]

    @staticmethod
    def process_index(index_name: str, df: pd.DataFrame):
        df.index = [row.lower().replace(" ", "_") for row in df.index.to_list()]
        df.index.name = index_name

        return df

    @staticmethod
    def process_columns_level_one(col_name: str):
        new_col_name = col_name.split(" (")[0]
        new_col_name = new_col_name.lower()
        new_col_name = new_col_name.replace(" ", "_")

        return new_col_name

    def read_src_file(self, sheet_name: str, header: List or None = None, index_col: List or None = None):
        if index_col is None:
            index_col = []
        if header is None:
            header = []

        _df = pd.read_excel(
            io=self.src_file,
            sheet_name=sheet_name,
            header=header,
            index_col=index_col,
        )

        return _df

    def get_level_one_to_name(self, df: pd.DataFrame):
        level_one_column = set(
            [col for col in set(df.columns.get_level_values(0))]
        )

        name = self.process_columns_level_one(col_name=level_one_column.pop())

        return name

    def process_df_sub(self, df: pd.DataFrame, level_one_col: str):
        temp_columns = [tuple_col for tuple_col in df.columns.to_list() if tuple_col[0] == level_one_col]

        df_temp = df[temp_columns]
        name = self.get_level_one_to_name(df=df_temp)

        level_two_columns = df_temp.columns.get_level_values(1)
        df_temp.columns = [col.split("YE")[0] for col in set(level_two_columns)]

        # sort columns
        columns_sorted = sorted(df_temp.columns)

        df_temp = df_temp[columns_sorted]
        df_temp.name = name

        return df_temp

    def split_df_mega_to_df_subs(self, df: pd.DataFrame):
        list_of_df_subs = []

        for level_one_col in set(df.columns.get_level_values(0)):
            df_temp = self.process_df_sub(df=df, level_one_col=level_one_col)
            list_of_df_subs.append(df_temp)

        return list_of_df_subs

    def write_df_to_file(self, df: pd.DataFrame):
        df.to_csv(self.output_folder + df.name + ".csv")

    def run(self):
        for sheet_row in self.sheet_details:
            _df_mega = self.read_src_file(
                sheet_name=sheet_row["sheet_name"],
                header=sheet_row["header"],
                index_col=sheet_row["index_col"]
            )

            _df_mega = self.process_index(index_name=sheet_row["index_name"], df=_df_mega)

            list_of_df_subs = self.split_df_mega_to_df_subs(df=_df_mega)

            [self.write_df_to_file(df=df_ele) for df_ele in list_of_df_subs]
