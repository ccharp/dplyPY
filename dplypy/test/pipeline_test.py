import os

import pandas as pd
import numpy as np
import pytest

from dplypy.dplyframe import *
from dplypy.pipeline import *


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
    pd.testing.assert_frame_equal((df + head(3)).pandas_df, pandas_df.head(3))


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
    pd.testing.assert_frame_equal((df + tail(2)).pandas_df, pandas_df.tail(2))


def test_select():
    pandas_df1 = pd.DataFrame(
        np.array(([1, 2, 3], [1, 5, 6], [6, 7, 8])), columns=["col1", "col2", "col3"]
    )
    df1 = DplyFrame(pandas_df1)

    output1 = df1 + select("col1 == 1")
    expected1 = pandas_df1.query("col1 == 1")
    pd.testing.assert_frame_equal(output1.pandas_df, expected1)

    output2 = df1 + select("col1 == 2")
    expected2 = pandas_df1.query("col1 == 2")
    pd.testing.assert_frame_equal(output2.pandas_df, expected2)

    output3 = df1 + select("col1 > col2")
    expected3 = pandas_df1.query("col1 > col2")
    pd.testing.assert_frame_equal(output3.pandas_df, expected3)

    output4 = df1 + select("col1 < col2")
    expected4 = pandas_df1.query("col1 < col2")
    pd.testing.assert_frame_equal(output4.pandas_df, expected4)

    output5 = df1 + select("col1 >= col2")
    expected5 = pandas_df1.query("col1 >= col2")
    pd.testing.assert_frame_equal(output5.pandas_df, expected5)

    output6 = df1 + select("col1 <= col2")
    expected6 = pandas_df1.query("col1 <= col2")
    pd.testing.assert_frame_equal(output6.pandas_df, expected6)

    output7 = df1 + select("col1 != col2")
    expected7 = pandas_df1.query("col1 != col2")
    pd.testing.assert_frame_equal(output7.pandas_df, expected7)

    output8 = df1 + select("col1 != col3 == 8")
    expected8 = pandas_df1.query("col1 != col3 == 8")
    pd.testing.assert_frame_equal(output8.pandas_df, expected8)

    try:
        df1 + select("col4 == 0")
    except pd.core.computation.ops.UndefinedVariableError:
        pass
    else:
        raise AssertionError("UndefinedVariableError was not raised")

    pandas_df2 = pd.DataFrame(
        np.array((["a", 1, 0], [1, 2, np.nan], [6, 7, 8])),
        columns=["col1", "col2", "col3"],
    )
    df2 = DplyFrame(pandas_df2)

    output9 = df2 + select("col1 > col2")
    expected9 = pandas_df2.query("col1 > col2")
    pd.testing.assert_frame_equal(output9.pandas_df, expected9)

    output10 = df2 + select("col2 != col3")
    expected10 = pandas_df2.query("col2 != col3")
    pd.testing.assert_frame_equal(output10.pandas_df, expected10)


def test_mutate():
    pandas_df1 = pd.DataFrame(np.array([[1, 2, 3], [1, 5, 6], [6, 7, 8]]))
    df1 = DplyFrame(pandas_df1)

    output1 = df1 + mutate(lambda x: x + 1)
    expected1 = pandas_df1.apply(lambda x: x + 1)
    pd.testing.assert_frame_equal(output1.pandas_df, expected1)

    output2 = df1 + mutate(np.sum)
    expected2 = pandas_df1.apply(np.sum)
    pd.testing.assert_series_equal(output2.pandas_df, expected2)

    output3 = df1 + mutate(np.sum, axis=1)
    expected3 = pandas_df1.apply(np.sum, axis=1)
    pd.testing.assert_series_equal(output3.pandas_df, expected3)

    try:
        df1 + mutate(1)
    except AssertionError:
        pass
    else:
        raise AssertionError("AssertionError was not raised")

    pandas_df2 = pd.DataFrame(np.array([["a", "b", "c"], [1, 5, np.nan], [6, 7, 8]]))
    df2 = DplyFrame(pandas_df2)

    output4 = df2 + mutate(np.argmax)
    expected4 = pandas_df2.apply(np.argmax)
    pd.testing.assert_series_equal(output4.pandas_df, expected4)

    try:
        df2 + mutate(lambda x: x + 1)
    except TypeError:
        pass
    else:
        raise AssertionError("TypeError was not raised")


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


def test_count_null():
    pandas_df1 = pd.DataFrame(
        data={"col1": [1, 2, 3, None], "col2": [1, 2, 3, 4], "col3": [None, 1, None, 2]}
    )

    df1 = DplyFrame(pandas_df1)

    # Default
    output1 = df1 + count_null()
    expected1 = 3
    assert output1 == expected1

    # count column
    output2 = df1 + count_null(column="col1")
    expected2 = 1
    assert output2 == expected2

    output3 = df1 + count_null(column="col2")
    expected3 = 0
    assert output3 == expected3

    output4 = df1 + count_null(column="col3")
    expected4 = 2
    assert output4 == expected4

    output5 = df1 + count_null(column=["col1", "col3"])
    expected5 = 3
    assert output5 == expected5

    # count index
    output6 = df1 + count_null(index=0)
    expected6 = 1
    assert output6 == expected6

    output7 = df1 + count_null(index=1)
    expected7 = 0
    assert output7 == expected7

    output8 = df1 + count_null(index=[0, 2])
    expected8 = 2
    assert output8 == expected8

    try:
        df1 + count_null(column="col4")
    except KeyError:
        pass
    else:
        raise AssertionError("KeyError was not raised")

    try:
        df1 + count_null(index=4)
    except KeyError:
        pass
    else:
        raise AssertionError("KeyError was not raised")

    # no missing value
    pandas_df2 = pd.DataFrame(data={"col5": [1, 2, 3, 4], "col6": [5, 6, 7, 8]})

    df2 = DplyFrame(pandas_df2)

    output9 = df2 + count_null()
    expected9 = 0
    assert output9 == expected9

    output10 = df2 + count_null(column="col5")
    expected10 = 0
    assert output10 == expected10

    output11 = df2 + count_null(index=0)
    expected11 = 0
    assert output11 == expected11


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
    series = pd.Series({"col1": 9, "col5": 10})
    output11 = df + fill_na(value=series)
    expected11 = pandas_df.fillna(value=series)
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


def test_join():
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

    # on
    output1 = df_l + join(df_r, on="common")
    expected1 = df_l.pandas_df.merge(df_r.pandas_df, on="common")
    pd.testing.assert_frame_equal(output1.pandas_df, expected1)

    # sort
    output2 = df_l + join(df_r, on="common", sort=True)
    expected2 = df_l.pandas_df.merge(df_r.pandas_df, on="common", sort=True)
    pd.testing.assert_frame_equal(output2.pandas_df, expected2)

    # left_on, right_on, suffixes
    output3 = df_l + join(
        df_r, left_on="left_key", right_on="right_key", suffixes=("_foo_x", "_foo_y")
    )
    expected3 = df_l.pandas_df.merge(
        df_r.pandas_df,
        left_on="left_key",
        right_on="right_key",
        suffixes=("_foo_x", "_foo_y"),
    )
    pd.testing.assert_frame_equal(output3.pandas_df, expected3)

    try:
        df_l + join(df_r, left_on="left_key")
    except pd.errors.MergeError:
        pass
    else:
        raise AssertionError("joinError was not raised")

    try:
        df_l + join(df_r, right_on="right_key")
    except pd.errors.MergeError:
        pass
    else:
        raise AssertionError("joinError was not raised")

    try:
        df_l + join(
            df_r, left_on="left_key", right_on="right_key", suffixes=(None, None)
        )
    except ValueError:
        pass
    else:
        raise AssertionError("ValueError was not raised")

    # left_index, right_index
    output4 = df_l + join(df_r, left_index=True, right_index=True)
    expected4 = df_l.pandas_df.merge(df_r.pandas_df, left_index=True, right_index=True)
    pd.testing.assert_frame_equal(output4.pandas_df, expected4)

    output5 = df_l + join(df_r, left_on="left_key", right_index=True)
    expected5 = df_l.pandas_df.merge(
        df_r.pandas_df, left_on="left_key", right_index=True
    )
    pd.testing.assert_frame_equal(output5.pandas_df, expected5)

    output6 = df_l + join(df_r, left_index=True, right_on="right_key")
    expected6 = df_l.pandas_df.merge(
        df_r.pandas_df, left_index=True, right_on="right_key"
    )
    pd.testing.assert_frame_equal(output6.pandas_df, expected6)

    try:
        df_l + join(df_r, left_index=True)
    except pd.errors.MergeError:
        pass
    else:
        raise AssertionError("joinError was not raised")

    try:
        df_l + join(df_r, right_index=True)
    except pd.errors.MergeError:
        pass
    else:
        raise AssertionError("joinError was not raised")

    df_l_new = DplyFrame(
        pd.DataFrame(
            data={
                "col1": [1, 2, 3, 4],
                "col2": [9, 10, 11, 12],
                "col3": [17, 18, 19, 20],
            }
        )
    )
    df_r_new = DplyFrame(
        pd.DataFrame(
            data={"col1": [1, 2, 3, 5], "col4": [5, 6, 7, 8], "col5": [5, 6, 7, 8]}
        )
    )

    # how
    output7 = df_l_new + join(df_r_new, how="left")
    expected7 = df_l_new.pandas_df.merge(df_r_new.pandas_df, how="left")
    pd.testing.assert_frame_equal(output7.pandas_df, expected7)

    output8 = df_l_new + join(df_r_new, on="col1", how="left")
    expected8 = df_l_new.pandas_df.merge(df_r_new.pandas_df, on="col1", how="left")
    pd.testing.assert_frame_equal(output8.pandas_df, expected8)

    output9 = df_l_new + join(df_r_new, how="right")
    expected9 = df_l_new.pandas_df.merge(df_r_new.pandas_df, how="right")
    pd.testing.assert_frame_equal(output9.pandas_df, expected9)

    output10 = df_l_new + join(df_r_new, how="outer")
    expected10 = df_l_new.pandas_df.merge(df_r_new.pandas_df, how="outer")
    pd.testing.assert_frame_equal(output10.pandas_df, expected10)

    # cross (new for pandas 1.2.0)
    output11 = df_l_new + join(df_r_new, how="cross")
    expected11 = df_l_new.pandas_df.merge(df_r_new.pandas_df, how="cross")
    pd.testing.assert_frame_equal(output11.pandas_df, expected11)

    try:
        df_l_new + join(df_r_new, on="col1", how="cross")
    except pd.errors.MergeError:
        pass
    else:
        raise AssertionError("joinError was not raised")

    try:
        df_l_new + join(df_r_new, left_on="col2", right_on="col4", how="cross")
    except pd.errors.MergeError:
        pass
    else:
        raise AssertionError("JoinError was not raised")

    try:
        df_l_new + join(df_r_new, left_index=True, right_index=True, how="cross")
    except pd.errors.MergeError:
        pass
    else:
        raise AssertionError("joinError was not raised")


def test_write_file():
    pandas_df = pd.DataFrame(
        data={
            "col1": [0, 1, 2, 3],
            "col2": [3, 4, 5, 6],
            "col3": [6, 7, 8, 9],
            "col4": [9, 10, 11, 12],
        }
    )

    df = DplyFrame(pandas_df)

    # To csv file
    (
        df
        + write_file("df_no_index.csv", sep=",", index=False)
        + write_file("df_with_index.csv", sep=",", index=True)
    )
    read_df = pd.read_csv("df_no_index.csv", sep=",")
    pd.testing.assert_frame_equal(df.pandas_df, read_df)  # Without index
    os.remove("df_no_index.csv")

    read_df = pd.read_csv("df_with_index.csv", sep=",", index_col=0)
    pd.testing.assert_frame_equal(df.pandas_df, read_df)  # With index
    os.remove("df_with_index.csv")

    # To excel file
    # Requires dependency openpyxl
    (
        df
        + write_file("df_no_index.xlsx", index=False)
        + write_file("df_with_index.xlsx", index=True)
    )
    read_df = pd.read_excel("df_no_index.xlsx", engine="openpyxl")
    pd.testing.assert_frame_equal(df.pandas_df, read_df)  # Without index
    os.remove("df_no_index.xlsx")

    read_df = pd.read_excel("df_with_index.xlsx", index_col=0, engine="openpyxl")
    pd.testing.assert_frame_equal(df.pandas_df, read_df)  # Without index
    os.remove("df_with_index.xlsx")

    # To json
    df + write_file("df_no_index.json")
    read_df = pd.read_json("df_no_index.json")
    pd.testing.assert_frame_equal(df.pandas_df, read_df)  # Without index
    os.remove("df_no_index.json")

    # To pickle
    df + write_file("df_no_index.pkl")
    read_df = pd.read_pickle("df_no_index.pkl")
    pd.testing.assert_frame_equal(df.pandas_df, read_df)  # Without index
    os.remove("df_no_index.pkl")

    # Error case
    with pytest.raises(IOError):
        df + write_file("df.abc")


def test_side_effect(capsys):
    pandas_df = pd.DataFrame(
        data={
            "col1": ["a", "b", "c", "d"],
        }
    )
    dplyf = DplyFrame(pandas_df.copy(deep=True))

    # Verify that a side effect occured
    df_post_pipe = (
        dplyf
        + s(lambda df: print(df["col1"][1]))
        + side_effect(lambda df: print(df["col1"][2]))
    )
    expected_stdout = "b\nc\n"
    captured_stdout = capsys.readouterr().out
    assert expected_stdout == captured_stdout
    # Verify that the data frame was not modified
    pd.testing.assert_frame_equal(dplyf.pandas_df, df_post_pipe.pandas_df)

    # Verfy that side effects cannot modify the data frame
    def my_side_effect_func(d1):
        d1["new_column"] = 0

    df_post_pipe = dplyf + s(my_side_effect_func)
    pd.testing.assert_frame_equal(dplyf.pandas_df, df_post_pipe.pandas_df)


def test_gather():
    pandas_df = pd.DataFrame(
        {
            "A": {0: "a", 1: "b", 2: "c"},
            "B": {0: 1, 1: 3, 2: 5},
            "C": {0: 2, 1: 4, 2: 6},
        }
    )
    df = DplyFrame(pandas_df)

    # id_vars
    melted_pandas_df_1 = pd.melt(pandas_df, id_vars="A")
    melted_df_1 = df + gather(id_vars="A")
    pd.testing.assert_frame_equal(melted_pandas_df_1, melted_df_1.pandas_df)

    melted_pandas_df_2 = pd.melt(pandas_df, id_vars=["A", "B"])
    melted_df_2 = df + gather(id_vars=["A", "B"])
    pd.testing.assert_frame_equal(melted_pandas_df_2, melted_df_2.pandas_df)

    melted_pandas_df_3 = pd.melt(pandas_df, id_vars=np.array(["A", "B"]))
    melted_df_3 = df + gather(id_vars=np.array(["A", "B"]))
    pd.testing.assert_frame_equal(melted_pandas_df_3, melted_df_3.pandas_df)

    melted_pandas_df_4 = pd.melt(pandas_df, id_vars=("A", "B"))
    melted_df_4 = df + gather(id_vars=("A", "B"))
    pd.testing.assert_frame_equal(melted_pandas_df_4, melted_df_4.pandas_df)

    try:
        df + gather(id_vars=["A", "K"])
    except KeyError:
        pass
    else:
        raise KeyError("KeyError was not raised")

    # value_vars
    melted_pandas_df_5 = pd.melt(pandas_df, value_vars="B")
    melted_df_5 = df + gather(value_vars="B")
    pd.testing.assert_frame_equal(melted_pandas_df_5, melted_df_5.pandas_df)

    # This is not advisable but should work by choosing last variable
    melted_pandas_df_6 = pd.melt(pandas_df, value_vars="K")
    melted_df_6 = df + gather(value_vars="K")
    pd.testing.assert_frame_equal(melted_pandas_df_6, melted_df_6.pandas_df)

    # In contrast, this raises an error
    try:
        df + gather(value_vars=["K"])
    except KeyError:
        pass
    else:
        raise KeyError("KeyError was not raised")

    melted_pandas_df_7 = pd.melt(pandas_df, value_vars=["A", "B"])
    melted_df_7 = df + gather(value_vars=["A", "B"])
    pd.testing.assert_frame_equal(melted_pandas_df_7, melted_df_7.pandas_df)

    melted_pandas_df_8 = pd.melt(pandas_df, value_vars=("A", "B"))
    melted_df_8 = df + gather(value_vars=("A", "B"))
    pd.testing.assert_frame_equal(melted_pandas_df_8, melted_df_8.pandas_df)

    melted_pandas_df_9 = pd.melt(pandas_df, value_vars=np.array(["A", "B"]))
    melted_df_9 = df + gather(value_vars=np.array(["A", "B"]))
    pd.testing.assert_frame_equal(melted_pandas_df_9, melted_df_9.pandas_df)

    melted_pandas_df_10 = pd.melt(pandas_df, id_vars=["A"], value_vars=["B"])
    melted_df_10 = df + gather(id_vars=["A"], value_vars=["B"])
    pd.testing.assert_frame_equal(melted_pandas_df_10, melted_df_10.pandas_df)

    # Again, not advisable but should work
    melted_pandas_df_11 = pd.melt(pandas_df, id_vars=["A"], value_vars="D")
    melted_df_11 = df + gather(id_vars=["A"], value_vars="D")
    pd.testing.assert_frame_equal(melted_pandas_df_11, melted_df_11.pandas_df)

    # var_name
    melted_pandas_df_12 = pd.melt(
        pandas_df, id_vars=["A"], value_vars=["B", "C"], var_name="myVarname"
    )
    melted_df_12 = df + gather(
        id_vars=["A"], value_vars=["B", "C"], var_name="myVarname"
    )
    pd.testing.assert_frame_equal(melted_pandas_df_12, melted_df_12.pandas_df)

    melted_pandas_df_13 = pd.melt(
        pandas_df, id_vars=["A"], value_vars=["B"], var_name=["myVarname"]
    )
    melted_df_13 = df + gather(id_vars=["A"], value_vars=["B"], var_name=["myVarname"])
    pd.testing.assert_frame_equal(melted_pandas_df_13, melted_df_13.pandas_df)

    try:
        df + gather(
            id_vars=["A"], value_vars=["B"], var_name=["myValname", "myValname"]
        )
    except IndexError:
        pass
    else:
        raise IndexError("IndexError was not raised")

    # value_name
    melted_pandas_df_13 = pd.melt(
        pandas_df, id_vars=["A"], value_vars=["B", "C"], value_name="myValname"
    )
    melted_df_13 = df + gather(
        id_vars=["A"], value_vars=["B", "C"], value_name="myValname"
    )
    pd.testing.assert_frame_equal(melted_pandas_df_13, melted_df_13.pandas_df)

    melted_pandas_df_14 = pd.melt(
        pandas_df,
        id_vars=["A"],
        value_vars=["B"],
        var_name="myVarname",
        value_name="myValname",
    )
    melted_df_14 = df + gather(
        id_vars=["A"], value_vars=["B"], var_name="myVarname", value_name="myValname"
    )
    pd.testing.assert_frame_equal(melted_pandas_df_14, melted_df_14.pandas_df)

    try:
        df + gather(id_vars=["A"], value_vars=["B"], value_name=["myValname"])
    except TypeError:
        pass
    else:
        raise TypeError("TypeError was not raised")

    # ignore_index
    melted_pandas_df_15 = pd.melt(
        pandas_df,
        id_vars=["A"],
        value_vars=["B"],
        var_name="myVarname",
        value_name=1234,
        ignore_index=False,
    )
    melted_df_15 = df + gather(
        id_vars=["A"],
        value_vars=["B"],
        var_name="myVarname",
        value_name=1234,
        ignore_index=False,
    )
    pd.testing.assert_frame_equal(melted_pandas_df_15, melted_df_15.pandas_df)


def test_one_hot():
    pandas_df = pd.DataFrame(
        {
            "col1": ["B", "A", "C", "D"],
            "col2": ["A", "B", "A", np.nan],
            "col3": [1, 2, 3, 4],
        }
    )

    df = DplyFrame(pandas_df)

    output1 = df + one_hot()
    expected1 = pd.get_dummies(pandas_df)
    pd.testing.assert_frame_equal(output1.pandas_df, expected1)

    output2 = df + one_hot(prefix="p")
    expected2 = pd.get_dummies(pandas_df, prefix="p")
    pd.testing.assert_frame_equal(output2.pandas_df, expected2)

    prefix_lst = ["p", "pp"]
    output3 = df + one_hot(prefix=prefix_lst)
    expected3 = pd.get_dummies(pandas_df, prefix=prefix_lst)
    pd.testing.assert_frame_equal(output3.pandas_df, expected3)

    prefix_dict = {"col1": 1, "col2": 2}
    output4 = df + one_hot(prefix=prefix_dict)
    expected4 = pd.get_dummies(pandas_df, prefix=prefix_dict)
    pd.testing.assert_frame_equal(output4.pandas_df, expected4)

    output5 = df + one_hot(prefix_sep="__")
    expected5 = pd.get_dummies(pandas_df, prefix_sep="__")
    pd.testing.assert_frame_equal(output5.pandas_df, expected5)

    output6 = df + one_hot(prefix="p", prefix_sep="__")
    expected6 = pd.get_dummies(pandas_df, prefix="p", prefix_sep="__")
    pd.testing.assert_frame_equal(output6.pandas_df, expected6)

    prefix_sep_lst = ["__", "___"]
    output7 = df + one_hot(prefix="p", prefix_sep=prefix_sep_lst)
    expected7 = pd.get_dummies(pandas_df, prefix="p", prefix_sep=prefix_sep_lst)
    pd.testing.assert_frame_equal(output7.pandas_df, expected7)

    prefix_sep_dict = {"col1": "___", "col2": "__"}
    output8 = df + one_hot(prefix="p", prefix_sep=prefix_sep_dict)
    expected8 = pd.get_dummies(pandas_df, prefix="p", prefix_sep=prefix_sep_dict)
    pd.testing.assert_frame_equal(output8.pandas_df, expected8)

    output9 = df + one_hot(columns=["col1"])
    expected9 = pd.get_dummies(pandas_df, columns=["col1"])
    pd.testing.assert_frame_equal(output9.pandas_df, expected9)

    output10 = df + one_hot(columns=["col1", "col3"])
    expected10 = pd.get_dummies(pandas_df, columns=["col1", "col3"])
    pd.testing.assert_frame_equal(output10.pandas_df, expected10)

    output11 = df + one_hot(drop_first=True)
    expected11 = pd.get_dummies(pandas_df, drop_first=True)
    pd.testing.assert_frame_equal(output11.pandas_df, expected11)

    output12 = df + one_hot(dummy_na=True)
    expected12 = pd.get_dummies(pandas_df, dummy_na=True)
    pd.testing.assert_frame_equal(output12.pandas_df, expected12)

    output13 = df + one_hot(dtype=int)
    expected13 = pd.get_dummies(pandas_df, dtype=int)
    pd.testing.assert_frame_equal(output13.pandas_df, expected13)

    output14 = df + one_hot(dtype=(str, int))
    expected14 = pd.get_dummies(pandas_df, dtype=(str, int))
    pd.testing.assert_frame_equal(output14.pandas_df, expected14)

    try:
        df + one_hot(prefix=1)
    except TypeError:
        pass
    else:
        raise AssertionError("TypeError was not raised")

    try:
        df + one_hot(prefix_sep=1)
    except TypeError:
        pass
    else:
        raise AssertionError("TypeError was not raised")

    try:
        df + one_hot(columns="col1")
    except TypeError:
        pass
    else:
        raise AssertionError("TypeError was not raised")

    try:
        df + one_hot(prefix=["p", "pp"], columns=["col1", "col2", "col3"])
    except ValueError:
        pass
    else:
        raise AssertionError("ValueError was not raised")

    try:
        df + one_hot(dtype=(int, str))
    except ValueError:
        pass
    else:
        raise AssertionError("ValueError was not raised")

    try:
        df + one_hot(dtype=[str, int])
    except TypeError:
        pass
    else:
        raise AssertionError("TypeError was not raised")


def test_pivot_table():
    pandas_df = pd.DataFrame(
        {
            "A": ["foo", "foo", "foo", "foo", "foo", "bar", "bar", "bar", "bar"],
            "B": ["one", "one", "one", "two", "two", "one", "one", "two", "two"],
            "C": [
                "small",
                "large",
                "large",
                "small",
                "small",
                "large",
                "small",
                "small",
                "large",
            ],
            "D": [1, 2, 2, 3, 3, 4, 5, 6, 7],
            "E": [2, 4, 5, 5, 6, 6, 8, 9, 9],
            "F": [np.nan for i in range(9)],
        }
    )

    df = DplyFrame(pandas_df)

    # General test
    output1 = df + pivot_table(index=["A", "B"], columns=["C"])
    expected1 = pandas_df.pivot_table(index=["A", "B"], columns=["C"])
    pd.testing.assert_frame_equal(output1.pandas_df, expected1)

    # values
    output2 = df + pivot_table(values="D", index=["A", "B"], columns=["C"])
    expected2 = pandas_df.pivot_table(values="D", index=["A", "B"], columns=["C"])
    pd.testing.assert_frame_equal(output2.pandas_df, expected2)

    output3 = df + pivot_table(values=["D", "F"], index=["A", "B"], columns=["C"])
    expected3 = pandas_df.pivot_table(
        values=["D", "F"], index=["A", "B"], columns=["C"]
    )
    pd.testing.assert_frame_equal(output3.pandas_df, expected3)

    # index
    output4 = df + pivot_table(index="A")
    expected4 = pandas_df.pivot_table(index="A")
    pd.testing.assert_frame_equal(output4.pandas_df, expected4)

    output5 = df + pivot_table(index=["A", "B"])
    expected5 = pandas_df.pivot_table(index=["A", "B"])
    pd.testing.assert_frame_equal(output5.pandas_df, expected5)

    output6 = df + pivot_table(index=np.arange(len(pandas_df)))
    expected6 = pandas_df.pivot_table(index=np.arange(len(pandas_df)))
    pd.testing.assert_frame_equal(output6.pandas_df, expected6)

    try:
        df + pivot_table(index=np.arange(len(pandas_df) - 1))
    except ValueError:
        pass
    else:
        raise AssertionError("ValueError was not raised")

    try:
        df + pivot_table(index=[["A"]])
    except ValueError:
        pass
    else:
        raise AssertionError("ValueError was not raised")

    try:
        df + pivot_table(values="A", index="A")
    except ValueError:
        pass
    else:
        raise AssertionError("ValueError was not raised")

    output7 = df + pivot_table(columns="C")
    expected7 = pandas_df.pivot_table(columns="C")
    pd.testing.assert_frame_equal(output7.pandas_df, expected7)

    output8 = df + pivot_table(columns=["C", "D"])
    expected8 = pandas_df.pivot_table(columns=["C", "D"])
    pd.testing.assert_frame_equal(output8.pandas_df, expected8)

    output9 = df + pivot_table(columns=np.arange(len(pandas_df)))
    expected9 = pandas_df.pivot_table(columns=np.arange(len(pandas_df)))
    pd.testing.assert_frame_equal(output9.pandas_df, expected9)

    try:
        df + pivot_table(columns=np.arange(len(pandas_df) - 1))
    except ValueError:
        pass
    else:
        raise AssertionError("ValueError was not raised")

    try:
        df + pivot_table(columns=[["A"]])
    except ValueError:
        pass
    else:
        raise AssertionError("ValueError was not raised")

    try:
        df + pivot_table(values="A", columns="A")
    except ValueError:
        pass
    else:
        raise AssertionError("ValueError was not raised")

    # fill_value
    output10 = df + pivot_table(
        values="D", index=["A", "B"], columns=["C"], fill_value=0
    )
    expected10 = pandas_df.pivot_table(
        values="D", index=["A", "B"], columns=["C"], fill_value=0
    )
    pd.testing.assert_frame_equal(output10.pandas_df, expected10)

    # Testing dropna
    output11 = df + pivot_table(
        values="D", index=["A", "B"], columns=["C"], dropna=False
    )
    expected11 = pandas_df.pivot_table(
        values="D", index=["A", "B"], columns=["C"], dropna=False
    )
    pd.testing.assert_frame_equal(output11.pandas_df, expected11)

    # Testing aggregation functions
    output12 = df + pivot_table(
        values="D", index=["A", "B"], columns=["C"], aggfunc=np.sum
    )
    expected12 = pandas_df.pivot_table(
        values="D", index=["A", "B"], columns=["C"], aggfunc=np.sum
    )
    pd.testing.assert_frame_equal(output12.pandas_df, expected12)

    output13 = df + pivot_table(
        values="D", index=["A", "B"], columns=["C"], aggfunc=np.std
    )
    expected13 = pandas_df.pivot_table(
        values="D", index=["A", "B"], columns=["C"], aggfunc=np.std
    )
    pd.testing.assert_frame_equal(output13.pandas_df, expected13)

    output14 = df + pivot_table(
        values="D", index=["A", "B"], columns=["C"], aggfunc=np.argmax
    )
    expected14 = pandas_df.pivot_table(
        values="D", index=["A", "B"], columns=["C"], aggfunc=np.argmax
    )
    pd.testing.assert_frame_equal(output14.pandas_df, expected14)

    output15 = df + pivot_table(
        values="D", index=["A", "B"], columns=["C"], aggfunc=np.argmin
    )
    expected15 = pandas_df.pivot_table(
        values="D", index=["A", "B"], columns=["C"], aggfunc=np.argmin
    )
    pd.testing.assert_frame_equal(output15.pandas_df, expected15)

    output16 = df + pivot_table(
        values="D", index=["A", "B"], columns=["C"], aggfunc=[np.argmin]
    )
    expected16 = pandas_df.pivot_table(
        values="D", index=["A", "B"], columns=["C"], aggfunc=[np.argmin]
    )
    pd.testing.assert_frame_equal(output16.pandas_df, expected16)

    output17 = df + pivot_table(
        values=["D", "E"], index=["A", "B"], columns=["C"], aggfunc=[np.argmin]
    )
    expected17 = pandas_df.pivot_table(
        values=["D", "E"], index=["A", "B"], columns=["C"], aggfunc=[np.argmin]
    )
    pd.testing.assert_frame_equal(output17.pandas_df, expected17)

    output18 = df + pivot_table(
        values=["D", "E"], index=["A", "B"], columns=["C"], aggfunc={"D": max, "E": min}
    )
    expected18 = pandas_df.pivot_table(
        values=["D", "E"], index=["A", "B"], columns=["C"], aggfunc={"D": max, "E": min}
    )
    pd.testing.assert_frame_equal(output18.pandas_df, expected18)

    output19 = df + pivot_table(
        values=["D", "E"], index=["A", "B"], columns=["C"], aggfunc={"D": max}
    )
    expected19 = pandas_df.pivot_table(
        values=["D", "E"], index=["A", "B"], columns=["C"], aggfunc={"D": max}
    )
    pd.testing.assert_frame_equal(output19.pandas_df, expected19)

    output20 = df + pivot_table(
        values=["D", "E"],
        index=["A", "B"],
        columns=["C"],
        aggfunc={"D": [max, min], "E": [max]},
    )
    expected20 = pandas_df.pivot_table(
        values=["D", "E"],
        index=["A", "B"],
        columns=["C"],
        aggfunc={"D": [max, min], "E": [max]},
    )
    pd.testing.assert_frame_equal(output20.pandas_df, expected20)

    output21 = df + pivot_table(
        values=["D", "E"], index=["A", "B"], columns=["C"], aggfunc={"D": [min, min]}
    )
    expected21 = pandas_df.pivot_table(
        values=["D", "E"], index=["A", "B"], columns=["C"], aggfunc={"D": [min, min]}
    )
    pd.testing.assert_frame_equal(output21.pandas_df, expected21)

    try:
        df + pivot_table(
            values=["D", "E"], index=["A", "B"], columns=["C"], aggfunc=[[max, min]]
        )
    except TypeError:
        pass
    else:
        raise AssertionError("TypeError was not raised")

    try:
        df + pivot_table(
            values=["D", "E"], index=["A", "B"], columns=["C"], aggfunc={"F": max}
        )
    except KeyError:
        pass
    else:
        raise AssertionError("KeyError was not raised")


def test_filter():
    pandas_df = pd.DataFrame(
        {
            "A": ["foo", "foo", "foo", "foo", "foo", "bar", "bar", "bar", "bar"],
            "B": ["one", "one", "one", "two", "two", "one", "one", "two", "two"],
            "C": [
                "small",
                "large",
                "large",
                "small",
                "small",
                "large",
                "small",
                "small",
                "large",
            ],
            "D": [1, 2, 2, 3, 3, 4, 5, 6, 7],
            "E": [2, 4, 5, 5, 6, 6, 8, 9, 9],
            "F": [np.nan for i in range(9)],
        }
    )

    df = DplyFrame(pandas_df)

    output1 = df + filter(df["B"] == "one")
    expected1 = pandas_df[pandas_df["B"] == "one"]
    pd.testing.assert_frame_equal(output1.pandas_df, expected1)

    output2 = df + filter(df["D"] == 1)
    expected2 = pandas_df[pandas_df["D"] == 1]
    pd.testing.assert_frame_equal(output2.pandas_df, expected2)

    output3 = df + filter(df["D"] > 1)
    expected3 = pandas_df[pandas_df["D"] > 1]
    pd.testing.assert_frame_equal(output3.pandas_df, expected3)

    output4 = df + filter(df["D"] < 1)
    expected4 = pandas_df[pandas_df["D"] < 1]
    pd.testing.assert_frame_equal(output4.pandas_df, expected4)

    output5 = df + filter(df["D"] >= 1)
    expected5 = pandas_df[pandas_df["D"] >= 1]
    pd.testing.assert_frame_equal(output5.pandas_df, expected5)

    output6 = df + filter(df["D"] <= 1)
    expected6 = pandas_df[pandas_df["D"] <= 1]
    pd.testing.assert_frame_equal(output6.pandas_df, expected6)

    output7 = df + filter(df["D"] != 1)
    expected7 = pandas_df[pandas_df["D"] != 1]
    pd.testing.assert_frame_equal(output7.pandas_df, expected7)

    output8 = df + filter((df["D"] + df["E"]) != 1)
    expected8 = pandas_df[(pandas_df["D"] + pandas_df["E"]) != 1]
    pd.testing.assert_frame_equal(output8.pandas_df, expected8)

    output9 = df + filter((df["D"] - df["F"]) != 1)
    expected9 = pandas_df[(pandas_df["D"] - pandas_df["F"]) != 1]
    pd.testing.assert_frame_equal(output9.pandas_df, expected9)

    output10 = df + filter((df["D"] * df["F"]) != 1)
    expected10 = pandas_df[(pandas_df["D"] * pandas_df["F"]) != 1]
    pd.testing.assert_frame_equal(output10.pandas_df, expected10)

    output11 = df + filter((df["D"] / df["F"]) != 1)
    expected11 = pandas_df[(pandas_df["D"] / pandas_df["F"]) != 1]
    pd.testing.assert_frame_equal(output11.pandas_df, expected11)

    output12 = df + filter((df["D"] // df["E"]) != 1)
    expected12 = pandas_df[(pandas_df["D"] // pandas_df["E"]) != 1]
    pd.testing.assert_frame_equal(output12.pandas_df, expected12)

    output13 = df + filter((df["C"] == "small") & (df["E"] > 1))
    expected13 = pandas_df[(pandas_df["C"] == "small") & (pandas_df["E"] > 1)]
    pd.testing.assert_frame_equal(output13.pandas_df, expected13)

    output14 = df + filter((df["C"] == "small") | (df["E"] > 1))
    expected14 = pandas_df[(pandas_df["C"] == "small") | (pandas_df["E"] > 1)]
    pd.testing.assert_frame_equal(output14.pandas_df, expected14)

    output15 = df + filter(~(df["C"] == "small"))
    expected15 = pandas_df[~(pandas_df["C"] == "small")]
    pd.testing.assert_frame_equal(output15.pandas_df, expected15)

    try:
        df + filter(df["K"] == 0)
    except KeyError:
        pass
    else:
        raise AssertionError("KeyError was not raised")

    try:
        df + filter((df["C"] == "small") and (df["E"] > 1))
    except ValueError:
        pass
    else:
        raise AssertionError("ValueError was not raised")

    try:
        df + filter((df["C"] >> 2) > 1)
    except TypeError:
        pass
    else:
        raise AssertionError("TypeError was not raised")


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


if __name__ == "__main__":
    test_head()
    test_tail()
    test_select()
    test_mutate()
    test_count_null()
    test_drop_na()
    test_fill_na()
    test_join()
    test_write_file()
    test_gather()
    test_one_hot()
    test_pivot_table()
    test_filter()
    test_arrange()
    test_row_name_subset()
    test_slice_row()
    test_slice_column()
