"""DplyFrame represents the dataframe we want to transform."""
import pandas as pd


class DplyFrame:
    """
    pandas.DataFrame wrapper for implementing DyplyR style data pipelines.
    Here, we simulate dplyr's `%>%` with `+`.
    See pipeline.py for pipeline-function-specific documentation.
    """

    def __init__(self, pandas_df: pd.DataFrame):
        """
        Create a DplyFrame from a pandas DataFrame
        :param pandas_df: the data frame we wish to wrap
        """
        self.pandas_df = pandas_df

    def __getitem__(self, item):
        return self.pandas_df[item]

    def __setitem__(self, key, value):
        self.pandas_df[key] = value

    def __eq__(d1, other):
        return d1.pandas_df == other

    def __lt__(d1, other):
        return d1.pandas_df < other

    def __le__(d1, other):
        return d1.pandas_df <= other

    def __gt__(d1, other):
        return d1.pandas_df > other

    def __ge__(d1, other):
        return d1.pandas_df >= other

    def __ne__(d1, other):
        return not (d1.__eq__(d1, other))

    def __add__(d1, d2_func):
        """
        Chain two or more pipline operations together.
        Important: here, `+` operator is NOT commutative
        Example:
        ```
        read_csv("foo.csv") + drop(['X']) + query("A" > 1337) + write_csv("transformed_foo.csv")
        ```
        :param d1: self--the forst operand of `+`
        :param d2_func: lazily evaluated DplyFrame (DplyFrame wrapped in a function)
                  returned by a pipeline method
        """
        return d2_func(d1)

    def deep_copy(self):
        return DplyFrame(self.pandas_df.copy(deep=True))

    def __repr__(self):
        return self.pandas_df.to_string()
