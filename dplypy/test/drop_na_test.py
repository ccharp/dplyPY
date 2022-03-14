import pandas as pd
import numpy as np

from dplypy.dplypy import DplyFrame
from dplypy.pipeline import drop_na


def test_drop_na():
    pandas_df = pd.DataFrame(
        {
            "col1": ["A", "B", "C"],
            "col2": [np.nan, "D", "E"],
            "col3": [pd.NaT, "F", pd.NaT],
        }
    )
    df = DplyFrame(pandas_df)

    # Default
    output1 = df + drop_na()
    expected1 = pandas_df.dropna()
    pd.testing.assert_frame_equal(output1.pandas_df, expected1)

    # axes
    output2 = df + drop_na(axis=1)
    expected2 = pandas_df.dropna(axis=1)
    pd.testing.assert_frame_equal(output2.pandas_df, expected2)

    output3 = df + drop_na(axis="index")
    expected3 = pandas_df.dropna(axis="index")
    pd.testing.assert_frame_equal(output3.pandas_df, expected3)

    output4 = df + drop_na(axis="columns")
    expected4 = pandas_df.dropna(axis="columns")
    pd.testing.assert_frame_equal(output4.pandas_df, expected4)

    # how
    output5 = df + drop_na(how="all")
    expected5 = pandas_df.dropna(how="all")
    pd.testing.assert_frame_equal(output5.pandas_df, expected5)

    # threshold
    output6 = df + drop_na(thresh=2)
    expected6 = pandas_df.dropna(thresh=2)
    pd.testing.assert_frame_equal(output6.pandas_df, expected6)

    # subset
    output7 = df + drop_na(subset=["col1", "col2"])
    expected7 = pandas_df.dropna(subset=["col1", "col2"])
    pd.testing.assert_frame_equal(output7.pandas_df, expected7)

    output8 = df + drop_na(axis=1, subset=[2])
    expected8 = pandas_df.dropna(axis=1, subset=[2])
    pd.testing.assert_frame_equal(output8.pandas_df, expected8)

    output9 = df + drop_na(axis=1, subset=[2]) + drop_na(axis=1, subset=[1])
    expected9 = pandas_df.dropna(axis=1, subset=[1, 2])
    pd.testing.assert_frame_equal(output9.pandas_df, expected9)

    try:
        df + drop_na(subset=[2])
    except KeyError:
        pass
    else:
        raise AssertionError("KeyError was not raised")

    try:
        df + drop_na(subset=2)
    except KeyError:
        pass
    else:
        raise AssertionError("KeyError was not raised")
