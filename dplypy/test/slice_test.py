import pandas as pd
import numpy as np

from dplypy.dplyframe import DplyFrame
from dplypy.pipeline import row_name_subset, slice_row, slice_column


def test_row_name_subset():
    pandas_df = pd.DataFrame(
        [[1, 2], [3, 4], [5, 6]], index=["idx1", 7, "idx3"], columns=["col1", "col2"]
    )
    df = DplyFrame(pandas_df)

    output1 = df + row_name_subset(["idx1"])
    expected1 = pandas_df.loc[["idx1"]]
    pd.testing.assert_frame_equal(output1.pandas_df, expected1)

    output2 = df + row_name_subset([7])
    expected2 = pandas_df.loc[[7]]
    pd.testing.assert_frame_equal(output2.pandas_df, expected2)

    try:
        df + row_name_subset([6])
    except KeyError:
        pass
    else:
        raise AssertionError("KeyError was not raised")

    output3 = df + row_name_subset([7, "idx3"])
    expected3 = pandas_df.loc[[7, "idx3"]]
    pd.testing.assert_frame_equal(output3.pandas_df, expected3)

    output4 = df + row_name_subset(
        pd.Series([False, True, False], index=[7, "idx1", "idx3"])
    )
    expected4 = pandas_df.loc[
        pd.Series([False, True, False], index=[7, "idx1", "idx3"])
    ]
    pd.testing.assert_frame_equal(output4.pandas_df, expected4)

    try:
        df + row_name_subset(pd.Series([False, True, False], index=[6, "idx1", "idx3"]))
    except pd.core.indexing.IndexingError:
        pass
    else:
        raise AssertionError("IndexingError was not raised")

    output5 = df + row_name_subset(pd.Index(["idx3", 7], name="indices"))
    expected5 = pandas_df.loc[pd.Index(["idx3", 7], name="indices")]
    pd.testing.assert_frame_equal(output5.pandas_df, expected5)


def test_slice_row():
    pandas_df = pd.DataFrame(
        index=[4, 6, 2, 1],
        columns=["a", "b", "c"],
        data=[[1, 2, 3], [4, 5, 6], [7, 8, 9], [10, 11, 12]],
    )
    df = DplyFrame(pandas_df)

    output1 = df + slice_row([0])
    expected1 = pandas_df.iloc[[0]]
    pd.testing.assert_frame_equal(output1.pandas_df, expected1)

    try:
        df + slice_row([4])
    except IndexError:
        pass
    else:
        raise AssertionError("IndexError was not raised")

    output2 = df + slice_row([1, 2])
    expected2 = pandas_df.iloc[[1, 2]]
    pd.testing.assert_frame_equal(output2.pandas_df, expected2)

    output3 = df + slice_row([True, False, False, True])
    expected3 = pandas_df.iloc[[True, False, False, True]]
    pd.testing.assert_frame_equal(output3.pandas_df, expected3)

    try:
        df + slice_row([True])
    except IndexError:
        pass
    else:
        raise AssertionError("IndexError was not raised")

    output4 = df + slice_row(np.arange(1, 4))
    expected4 = pandas_df.iloc[1:4]
    pd.testing.assert_frame_equal(output4.pandas_df, expected4)

    output5 = df + slice_row(lambda x: x.index % 2 == 1)
    expected5 = pandas_df.iloc[lambda x: x.index % 2 == 1]
    pd.testing.assert_frame_equal(output5.pandas_df, expected5)

    output6 = df + slice_row(1, 3)
    expected6 = pandas_df.iloc[1:3]
    pd.testing.assert_frame_equal(output6.pandas_df, expected6)

    output7 = df + slice_row(0, 4)
    expected7 = pandas_df.iloc[0:4]
    pd.testing.assert_frame_equal(output7.pandas_df, expected7)

    # Not advisable but should work
    output8 = df + slice_row(1, 5)
    expected8 = pandas_df.iloc[1:5]
    pd.testing.assert_frame_equal(output8.pandas_df, expected8)


def test_slice_column():
    pandas_df = pd.DataFrame(
        {
            "col1": ["A", "A", "A", "B", "B", "B"],
            "col2": [1, 3, 5, 2, 4, 6],
            "col3": [1, 3, 3, 5, 6, 4],
            "col4": [6, 3, 4, 2, 5, 0],
        }
    )
    df = DplyFrame(pandas_df)

    output1 = df + slice_column([1])
    expected1 = pandas_df.iloc[:, [1]]
    pd.testing.assert_frame_equal(output1.pandas_df, expected1)

    try:
        df + slice_column([5])
    except IndexError:
        pass
    else:
        raise AssertionError("IndexError was not raised")

    output2 = df + slice_column([0, 2])
    expected2 = pandas_df.iloc[:, [0, 2]]
    pd.testing.assert_frame_equal(output2.pandas_df, expected2)

    output3 = df + slice_column([True, False, False, True])
    expected3 = pandas_df.iloc[:, [True, False, False, True]]
    pd.testing.assert_frame_equal(output3.pandas_df, expected3)

    try:
        df + slice_row([False])
    except IndexError:
        pass
    else:
        raise AssertionError("IndexError was not raised")

    output4 = df + slice_column(np.arange(1, 3))
    expected4 = pandas_df.iloc[:, 1:3]
    pd.testing.assert_frame_equal(output4.pandas_df, expected4)

    output5 = df + slice_column(lambda x: [2, 3])
    expected5 = pandas_df.iloc[:, lambda x: [2, 3]]
    pd.testing.assert_frame_equal(output5.pandas_df, expected5)

    output6 = df + slice_column(0, 4)
    expected6 = pandas_df.iloc[:, 0:4]
    pd.testing.assert_frame_equal(output6.pandas_df, expected6)

    output7 = df + slice_column(1, 3)
    expected7 = pandas_df.iloc[:, 1:3]
    pd.testing.assert_frame_equal(output7.pandas_df, expected7)

    # not advisable but should work
    output8 = df + slice_column(0, 5)
    expected8 = pandas_df.iloc[:, 0:5]
    pd.testing.assert_frame_equal(output8.pandas_df, expected8)
