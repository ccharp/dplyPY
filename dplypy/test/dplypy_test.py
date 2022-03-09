import pandas as pd

from dplypy.dplypy import DplyFrame


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


def test_head():
    pandas_df = pd.DataFrame(
        data={
            "col1": [0, 1, 2, 3],
            "col2": [2, 3, 4, 5],
            "col3": [4, 5, 6, 7],
            "col4": [6, 7, 8, 9],
        }
    )
    df = DplyFrame(pandas_df)
    pd.testing.assert_frame_equal(df.head(3).pandas_df, pandas_df.head(3))


def test_tail():
    pandas_df = pd.DataFrame(
        data={
            "col1": [3, 2, 1, 0],
            "col2": [5, 4, 3, 2],
            "col3": [7, 6, 5, 4],
            "col4": [9, 8, 7, 6],
        }
    )
    df = DplyFrame(pandas_df)
    pd.testing.assert_frame_equal(df.tail(2).pandas_df, pandas_df.tail(2))
