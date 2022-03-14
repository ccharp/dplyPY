import pandas as pd
import numpy as np

from dplypy.dplyframe import DplyFrame
from dplypy.pipeline import arrange


def test_arrange():
    pandas_df = pd.DataFrame(
        data=[[5, 1, 0], [20, 2, 2], [0, 8, 8], [np.nan, 7, 9], [10, 7, 5], [15, 4, 3]],
        columns=["col1", "col2", "col3"],
        index=[1, 3, 5, 7, 9, 11],
    )
    df = DplyFrame(pandas_df)

    output1 = df + arrange(by="col1")
    expected1 = pandas_df.sort_values(by="col1")
    pd.testing.assert_frame_equal(output1.pandas_df, expected1)

    try:
        df + arrange(by=1)
    except KeyError:
        pass
    else:
        raise AssertionError("KeyError was not raised")

    output2 = df + arrange(by=["col1", "col2"], ascending=False)
    expected2 = pandas_df.sort_values(by=["col1", "col2"], ascending=False)
    pd.testing.assert_frame_equal(output2.pandas_df, expected2)

    try:
        df + arrange(by=["col1", "col4"])
    except KeyError:
        pass
    else:
        raise AssertionError("KeyError was not raised")

    output3 = df + arrange(by=["col1"], ascending=False)
    expected3 = pandas_df.sort_values(by=["col1"], ascending=False)
    pd.testing.assert_frame_equal(output3.pandas_df, expected3)

    output4 = df + arrange(by="col1", axis="index")
    expected4 = pandas_df.sort_values(by="col1", axis="index")
    pd.testing.assert_frame_equal(output4.pandas_df, expected4)

    output5 = df + arrange(by=1, axis=1)
    expected5 = pandas_df.sort_values(by=1, axis=1)
    pd.testing.assert_frame_equal(output5.pandas_df, expected5)

    output6 = df + arrange(by=[1, 3], axis="columns", ascending=[True, False])
    expected6 = pandas_df.sort_values(
        by=[1, 3], axis="columns", ascending=[True, False]
    )
    pd.testing.assert_frame_equal(output6.pandas_df, expected6)
