import pandas as pd


from src.analytics.class_firm_size import FirmSizeAnalyticClass
from src.analytics.class_changing_business_profile import ChangingBusinessProfileAnalyticClass
from src.analytics.class_outliers import OutliersAnalyticClass


def get_df_mega_sorted_columns(source_file: str):
    xls = pd.ExcelFile(source_file)

    list_of_df = []

    for sheet_name in xls.sheet_names:
        df_temp = pd.read_excel(
            io=source_file,
            sheet_name=sheet_name,
            index_col=[0],
            header=[0, 1]
        )

        list_of_df.append(df_temp)

    df_mega = pd.concat(list_of_df, axis=1)

    df_mega = df_mega.reindex(["2016YE", "2017YE", "2018YE", "2019YE", "2020YE"], level=1, axis=1)

    return df_mega


def rename_all_columns_df_mega(df: pd.DataFrame):
    def process_columns_level_one(col_name: str):
        new_col_name = col_name.split(" (")[0]
        new_col_name = new_col_name.lower()
        new_col_name = new_col_name.replace(" ", "_")

        return new_col_name

    new_name_column_level_zero = {
        col: process_columns_level_one(col_name=col) for col in df.columns.get_level_values(0)
    }

    new_name_column_level_one = {
        col: col.split("YE")[0] for col in df.columns.get_level_values(1)
    }

    df = df.rename(columns=new_name_column_level_zero, level=0)
    df = df.rename(columns=new_name_column_level_one, level=1)

    return df


def main():
    source_file = (
        "./source_files/big_file/Data for technical assessment.xlsx"
    )
    df_mega = get_df_mega_sorted_columns(source_file=source_file)
    df_mega = rename_all_columns_df_mega(df=df_mega)

    output_folder = "./output_files"

    firm_size_list = FirmSizeAnalyticClass(
        df_mega=df_mega,
        firm_metrics={
            "gwp": [0.5, True],
            "total_assets": [0.5, True],
        },
        output_folder=output_folder
    ).run()

    print(f"firm size list in descending order {firm_size_list}")

    changing_business_profile_list = ChangingBusinessProfileAnalyticClass(
        df_mega=df_mega,
        firm_metrics={
            "gross_claims_incurred": [0.5, False],
            "gwp": [0.5, True],
        },
        output_folder=output_folder
    ).run()

    print(f"changing business profile list in descending order {changing_business_profile_list}")

    outlier_list = OutliersAnalyticClass(
        df_mega=df_mega,
        firm_metrics={
            "gwp": [0.33, True],
            "nwp": [0.33, False],
            "scr_coverage_ratio": [0.33, False]
        },
        output_folder=output_folder
    ).run()

    print(f"outlier list in descending order {outlier_list}")


if __name__ == '__main__':
    main()


