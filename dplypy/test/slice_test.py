import pandas as pd
import numpy as np

from dplypy.dplypy import DplyFrame, row_name_subset, slice


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
        df + \
            row_name_subset(
                pd.Series([False, True, False], index=[6, "idx1", "idx3"]))
    except pd.core.indexing.IndexingError:
        pass
    else:
        raise AssertionError("IndexingError was not raised")

    output5 = df + row_name_subset(pd.Index(["idx3", 7], name="indices"))
    expected5 = pandas_df.loc[pd.Index(["idx3", 7], name="indices")]
    pd.testing.assert_frame_equal(output5.pandas_df, expected5)


def test_slice():
    pandas_df = pd.DataFrame(
        index=[4, 6, 2, 1],
        columns=["a", "b", "c"],
        data=[[1, 2, 3], [4, 5, 6], [7, 8, 9], [10, 11, 12]],
    )
    df = DplyFrame(pandas_df)

    output1 = df + slice([0])
    expected1 = pandas_df.iloc[[0]]
    pd.testing.assert_frame_equal(output1.pandas_df, expected1)

    try:
        df + slice([4])
    except IndexError:
        pass
    else:
        raise AssertionError("IndexError was not raised")

    output2 = df + slice([1, 2])
    expected2 = pandas_df.iloc[[1, 2]]
    pd.testing.assert_frame_equal(output2.pandas_df, expected2)

    output3 = df + slice([True, False, False, True])
    expected3 = pandas_df.iloc[[True, False, False, True]]
    pd.testing.assert_frame_equal(output3.pandas_df, expected3)

    try:
        df + slice([True])
    except IndexError:
        pass
    else:
        raise AssertionError("IndexError was not raised")

    output4 = df + slice(np.arange(1, 4))
    expected4 = pandas_df.iloc[1:4]
    pd.testing.assert_frame_equal(output4.pandas_df, expected4)

    output5 = df + slice(lambda x: x.index % 2 == 1)
    expected5 = pandas_df.iloc[lambda x: x.index % 2 == 1]
    pd.testing.assert_frame_equal(output5.pandas_df, expected5)
