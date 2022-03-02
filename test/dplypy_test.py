import pandas as pd
from src.dplypy import DplyFrame
import numpy as np
import os
import pytest


def test_init():
    pandas_df = pd.DataFrame(
        data={
            "col1": [0, 1, 2, 3],
            "col2": [3, 4, 5, 6],
            "col3": [6, 7, 8, 9],
            "col4": [9, 10, 11, 12],
        }
    )

    # Drop by columns
    df1 = DplyFrame(pandas_df)
    pd.testing.assert_frame_equal(df1.pandas_df, pandas_df)


def test_getitem():
    pandas_df = pd.DataFrame(
        data={
            "col1": [0, 1, 2, 3],
            "col2": [3, 4, 5, 6],
            "col3": [6, 7, 8, 9],
            "col4": [9, 10, 11, 12],
        }
    )

    df1 = DplyFrame(pandas_df)
    pd.testing.assert_series_equal(pandas_df["col2"], df1["col2"])
    pd.testing.assert_frame_equal(
        pandas_df[pandas_df["col2"] > 4], df1[df1["col2"] > 4]
    )


def test_add():
    pandas_df = pd.DataFrame(
        data={
            "col1": [0, 1, 2, 3],
            "col2": [3, 4, 5, 6],
            "col3": [6, 7, 8, 9],
            "col4": [9, 10, 11, 12],
        }
    )

    df = DplyFrame(pandas_df)

    def add1():
        return lambda d1: DplyFrame(d1.pandas_df + 1)

    df_output = df + add1() + add1() + add1()
    pd.testing.assert_frame_equal(df_output.pandas_df, pandas_df + 3)


def test_repr():
    pandas_df = pd.DataFrame(
        data={
            "col1": [0, 1, 2, 3],
        }
    )

    df = DplyFrame(pandas_df)
    assert pandas_df.to_string() == df.__repr__()
