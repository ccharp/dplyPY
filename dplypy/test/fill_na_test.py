import pandas as pd
import numpy as np

from dplypy.dplypy import DplyFrame, fill_na


def test_fill_na():
    pandas_df = pd.DataFrame(
        [
            [np.nan, 1, np.nan, 2],
            [3, 4, np.nan, 5],
            [np.nan, np.nan, np.nan, np.nan],
            [np.nan, 6, np.nan, 7],
        ],
        columns=["col1", "col2", "col3", "col4"],
    )
    df = DplyFrame(pandas_df)

    # single value
    output1 = df + fill_na(0)
    expected1 = pandas_df.fillna(0)
    pd.testing.assert_frame_equal(output1.pandas_df, expected1)

    # method
    output2 = df + fill_na("backfill")
    expected2 = pandas_df.fillna("backfill")
    pd.testing.assert_frame_equal(output2.pandas_df, expected2)

    output3 = df + fill_na("bfill")
    expected3 = pandas_df.fillna("bfill")
    pd.testing.assert_frame_equal(output3.pandas_df, expected3)

    output4 = df + fill_na("pad")
    expected4 = pandas_df.fillna("pad")
    pd.testing.assert_frame_equal(output4.pandas_df, expected4)

    output5 = df + fill_na("ffill")
    expected5 = pandas_df.fillna("ffill")
    pd.testing.assert_frame_equal(output5.pandas_df, expected5)

    # axis
    output6 = df + fill_na(method="ffill", axis=1)
    expected6 = pandas_df.fillna(method="ffill", axis=1)
    pd.testing.assert_frame_equal(output6.pandas_df, expected6)

    # method + limit
    output7 = df + fill_na(method="ffill", limit=1)
    expected7 = pandas_df.fillna(method="ffill", limit=1)
    pd.testing.assert_frame_equal(output7.pandas_df, expected7)

    # value + limit
    output8 = df + fill_na(value=0, limit=1)
    expected8 = pandas_df.fillna(value=0, limit=1)
    pd.testing.assert_frame_equal(output8.pandas_df, expected8)

    # dictionary value
    d1 = {"col1": "a", "col2": 10, "col3": 11, "col4": 12}
    output9 = df + fill_na(value=d1)
    expected9 = pandas_df.fillna(value=d1)
    pd.testing.assert_frame_equal(output9.pandas_df, expected9)

    d2 = {"col1": 9, "col2": 10, "col3": 11, "col4": 12}
    d3 = {"col1": 9, "col2": 10, "col3": 11}
    d4 = {"col4": 12}
    output10 = df + fill_na(value=d3) + fill_na(value=d4)
    expected10 = pandas_df.fillna(value=d2)
    pd.testing.assert_frame_equal(output10.pandas_df, expected10)

    # series value
    se = pd.Series({"col1": 9, "col5": 10})
    output11 = df + fill_na(value=se)
    expected11 = pandas_df.fillna(value=se)
    pd.testing.assert_frame_equal(output11.pandas_df, expected11)

    # dataframe value
    new_df = pd.DataFrame(np.ones((4, 4)), columns=["col1", "col2", "A", "B"])
    output12 = df + fill_na(new_df)
    expected12 = pandas_df.fillna(new_df)
    pd.testing.assert_frame_equal(output12.pandas_df, expected12)

    try:
        df + fill_na(0, "ffill")
    except ValueError:
        pass
    else:
        raise AssertionError("ValueError was not raised")

    try:
        df + fill_na("ffill", limit=0)
    except ValueError:
        pass
    else:
        raise AssertionError("ValueError was not raised")

    try:
        df + fill_na(np.zeros((3, 3)))
    except ValueError:
        pass
    else:
        raise AssertionError("ValueError was not raised")
