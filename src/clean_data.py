import pandas as pd


class CleanDataHelper(object):
    INDICATOR = "indicator"

    @staticmethod
    def indicate_missing_data(row: pd.Series):
        """
        This function gives the divider value as long as the series is legit
        Legit series is if from 2016 to 2020 there is a series of non zero values
        For example legit series are:
            - 0, 0, 1, 1, 2
            - 1, 2, 1, 2, 1
        Non legit series are:
            - 0, 1, 0, 3, 5

        :param row: Series row which contains the values for years 2016 to 2020
        :return:
        """
        indicator = 0
        found_non_zero_value = False

        for val in row.values:
            if val > 0:
                indicator += 1
                found_non_zero_value = True
            else:
                if not found_non_zero_value:
                    continue
                else:
                    indicator = 0
                    break

        return indicator

    @classmethod
    def run(cls, df_sub: pd.DataFrame):
        # drop rows with negative values
        df_sub = df_sub[(df_sub >= 0)]
        df_sub = df_sub.dropna()

        # calculate indicator column
        df_sub[cls.INDICATOR] = df_sub.apply(lambda row: cls.indicate_missing_data(row), axis=1)

        df_sub = df_sub[df_sub[cls.INDICATOR] > 0]

        return df_sub


