import pandas as pd
from src.dplypy import DplyFrame
from src.pipeline import query, apply, drop, merge
import numpy as np


def _test():  # TODO: convert to pytest
    pandas_df = pd.DataFrame(
        np.array(([1, 2, 3], [1, 5, 6], [6, 7, 8])),
        index=["mouse", "rabbit", "owl"],
        columns=["col1", "col2", "col3"],
    )

    d = DplyFrame(pandas_df)
    output = d + apply(lambda x: x + 1) + query("col1 == 2")
    print(output.pandas_df)


def test_drop():
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
    output1 = df1 + drop(["col3", "col4"], axis=1)
    expected1 = pandas_df.drop(["col3", "col4"], axis=1)
    pd.testing.assert_frame_equal(output1.pandas_df, expected1)

    df2 = DplyFrame(pandas_df)
    output2 = df2 + drop(columns=["col3", "col4"])
    expected1 = pandas_df.drop(columns=["col3", "col4"])
    pd.testing.assert_frame_equal(output2.pandas_df, expected1)

    # Drop by rows
    df3 = DplyFrame(pandas_df)
    output3 = df3 + drop([0, 1])
    expected3 = pandas_df.drop([0, 1])
    pd.testing.assert_frame_equal(output3.pandas_df, expected3)

    df4 = DplyFrame(pandas_df)
    output4 = df4 + drop(index=[2, 3])
    expected4 = pandas_df.drop(index=[2, 3])
    pd.testing.assert_frame_equal(output4.pandas_df, expected4)


def test_merge():
    df_l = DplyFrame(
        pd.DataFrame(
            data={
                "common": [1, 2, 3, 4],
                "left_index": ["a", "b", "c", "d"],
                "left_key": [3, 4, 7, 6],
                "col3": [6, 7, 8, 9],
                "col4": [9, 10, 11, 12],
            }
        )
    )
    df_r = DplyFrame(
        pd.DataFrame(
            data={
                "common": [1, 2, 3, 4],
                "right_index": ["a", "b", "foo", "d"],
                "right_key": [3, 4, 5, 6],
                "col3": [1, 2, 3, 4],
                "col4": [5, 6, 7, 8],
            }
        )
    )

    output1 = df_l + merge(df_r, on="common")
    expected1 = df_l.pandas_df.merge(df_r.pandas_df, on="common")
    pd.testing.assert_frame_equal(output1.pandas_df, expected1)

    output1 = df_l + merge(df_r, on="common", sort=True)
    expected1 = df_l.pandas_df.merge(df_r.pandas_df, on="common", sort=True)
    pd.testing.assert_frame_equal(output1.pandas_df, expected1)

    output2 = df_l + merge(
        df_r, left_on="left_key", right_on="right_key", suffixes=("_foo_x", "_foo_y")
    )
    expected2 = df_l.pandas_df.merge(
        df_r.pandas_df,
        left_on="left_key",
        right_on="right_key",
        suffixes=("_foo_x", "_foo_y"),
    )
    pd.testing.assert_frame_equal(output2.pandas_df, expected2)

    output3 = df_l + merge(df_r, left_index=True, right_index=True)
    expected3 = df_l.pandas_df.merge(df_r.pandas_df, left_index=True, right_index=True)
    pd.testing.assert_frame_equal(output3.pandas_df, expected3)


if __name__ == "__main__":
    test_drop()
    test_merge()
