import pandas as pd

from dplypy.dplypy import DplyFrame, drop


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
    output1 = df1 + drop("col1", axis=1)
    expected1 = pandas_df.drop("col1", axis=1)
    pd.testing.assert_frame_equal(output1.pandas_df, expected1)

    df2 = DplyFrame(pandas_df)
    output2 = df2 + drop(["col3", "col4"], axis=1)
    expected2 = pandas_df.drop(["col3", "col4"], axis=1)
    pd.testing.assert_frame_equal(output2.pandas_df, expected2)

    try:
        df2 + drop(["col1", "col5"], axis=1)
    except KeyError:
        pass
    else:
        raise AssertionError("KeyError was not raised")

    df3 = DplyFrame(pandas_df)
    output3 = df3 + drop(columns="col1")
    expected3 = pandas_df.drop(columns="col1")
    pd.testing.assert_frame_equal(output3.pandas_df, expected3)

    df4 = DplyFrame(pandas_df)
    output4 = df4 + drop(columns=["col3", "col4"])
    expected4 = pandas_df.drop(columns=["col3", "col4"])
    pd.testing.assert_frame_equal(output4.pandas_df, expected4)

    df5 = DplyFrame(pandas_df)
    output5 = df5 + drop(columns=["col3", "col4"], axis=1)
    expected5 = pandas_df.drop(columns=["col3", "col4"], axis=1)
    pd.testing.assert_frame_equal(output5.pandas_df, expected5)

    try:
        df5 + drop(columns=["col1", "col5"])
    except KeyError:
        pass
    else:
        raise AssertionError("KeyError was not raised")

    # Drop by rows
    df6 = DplyFrame(pandas_df)
    output6 = df6 + drop(0)
    expected6 = pandas_df.drop(0)
    pd.testing.assert_frame_equal(output6.pandas_df, expected6)

    df7 = DplyFrame(pandas_df)
    output7 = df7 + drop([0, 1])
    expected7 = pandas_df.drop([0, 1])
    pd.testing.assert_frame_equal(output7.pandas_df, expected7)

    try:
        df7 + drop(4)
    except KeyError:
        pass
    else:
        raise AssertionError("KeyError was not raised")

    df8 = DplyFrame(pandas_df)
    output8 = df8 + drop(index=[2, 3]) + drop(index=1)
    expected8 = pandas_df.drop(index=[1, 2, 3])
    pd.testing.assert_frame_equal(output8.pandas_df, expected8)

    try:
        df8 + drop(index=[3, 4])
    except KeyError:
        pass
    else:
        raise AssertionError("KeyError was not raised")
