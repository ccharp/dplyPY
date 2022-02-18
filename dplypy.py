import pandas as pd
import functools as ft
import numpy as np
from typing import AnyStr, Callable


class DplyFrame:
    def __init__(self, pandas_df: pd.DataFrame):
        self.pandas_df = pandas_df
        self.index = pandas_df.index

    def __eq__(self, other: pd.DataFrame):
        return self.pandas_df == other.pandas_df

    # TODO: implement other boolean operators

    def __getitem__(self, item): # Item is polymorphic. Could be anything accepted by pd.DataFrame.__getitem__()
        return self.pandas_df[item]

    # TODO: comment exlpaining how this works
    def __add__(self, d2_func):
        return d2_func(self)
 
    def __repr__(self):
        self.pandas_df.to_string() # TODO: investigate: broken. Low priority


def read_csv():
    # TODO:
    return None
