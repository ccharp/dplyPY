import pandas as pd
from src.dplypy import DplyFrame
from src.pipeline import (
    query,
    apply,
    drop,
    merge,
    write_file,
    check_null_total,
    check_null_any,
    drop_na,
    fill_na,
)
import numpy as np
import os
import pytest


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
    output4 = df4 + drop(index=[2, 3]) + drop(index=[1])
    expected4 = pandas_df.drop(index=[1, 2, 3])
    pd.testing.assert_frame_equal(output4.pandas_df, expected4)


def test_check_null():
    pandas_df1 = pd.DataFrame(
        data={"col1": [1, 2, 3, None], "col2": [1, 2, 3, 4], "col3": [None, 1, None, 2]}
    )

    df1 = DplyFrame(pandas_df1)

    output1 = df1 + check_null_any()
    expected1 = True
    assert output1 == expected1

    output2 = df1 + check_null_any(column="col1")
    expected2 = True
    assert output2 == expected2

    output3 = df1 + check_null_any(column="col2")
    expected3 = False
    assert output3 == expected3

    output4 = df1 + check_null_any(column="col3")
    expected4 = True
    assert output4 == expected4

    try:
        df1 + check_null_any(column="col4")
    except KeyError:
        pass
    else:
        raise AssertionError("KeyError was not raised")

    output5 = df1 + check_null_total()
    expected5 = 3
    assert output5 == expected5

    output6 = df1 + check_null_total(column="col1")
    expected6 = 1
    assert output6 == expected6

    output7 = df1 + check_null_total(column="col2")
    expected7 = 0
    assert output7 == expected7

    output8 = df1 + check_null_total(column="col3")
    expected8 = 2
    assert output8 == expected8

    try:
        df1 + check_null_total(column="col4")
    except KeyError:
        pass
    else:
        raise AssertionError("KeyError was not raised")

    pandas_df2 = pd.DataFrame(data={"col5": [1, 2, 3, 4], "col6": [5, 6, 7, 8]})

    df2 = DplyFrame(pandas_df2)

    output9 = df2 + check_null_any()
    expected9 = False
    assert output9 == expected9

    output10 = df2 + check_null_any(column="col5")
    expected10 = False
    assert output10 == expected10

    output11 = df2 + check_null_total()
    expected11 = 0
    assert output11 == expected11

    output12 = df2 + check_null_total(column="col5")
    expected12 = 0
    assert output12 == expected12


def test_drop_na():
    pandas_df = pd.DataFrame(
        {
            "col1": ["A", "B", "C"],
            "col2": [np.nan, "D", "E"],
            "col3": [pd.NaT, "F", pd.NaT],
        }
    )
    df = DplyFrame(pandas_df)

    output1 = df + drop_na()
    expected1 = pandas_df.dropna()
    pd.testing.assert_frame_equal(output1, expected1)

    output2 = df + drop_na(axis=1)
    expected2 = pandas_df.dropna(axis=1)
    pd.testing.assert_frame_equal(output2, expected2)

    output3 = df + drop_na(axis="index")
    expected3 = pandas_df.dropna(axis="index")
    pd.testing.assert_frame_equal(output3, expected3)

    output4 = df + drop_na(axis="columns")
    expected4 = pandas_df.dropna(axis="columns")
    pd.testing.assert_frame_equal(output4, expected4)

    output5 = df + drop_na(how="all")
    expected5 = pandas_df.dropna(how="all")
    pd.testing.assert_frame_equal(output5, expected5)

    output6 = df + drop_na(thresh=2)
    expected6 = pandas_df.dropna(thresh=2)
    pd.testing.assert_frame_equal(output6, expected6)

    output7 = df + drop_na(subset=["col1", "col2"])
    expected7 = pandas_df.dropna(subset=["col1", "col2"])
    pd.testing.assert_frame_equal(output7, expected7)

    output8 = df + drop_na(axis=1, subset=[2])
    expected8 = pandas_df.dropna(axis=1, subset=[2])
    pd.testing.assert_frame_equal(output8, expected8)

    try:
        df + drop_na(subset=[2])
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

    output1 = df + fill_na(0)
    expected1 = pandas_df.fillna(0)
    pd.testing.assert_frame_equal(output1, expected1)

    output2 = df + fill_na("backfill")
    expected2 = pandas_df.fillna("backfill")
    pd.testing.assert_frame_equal(output2, expected2)

    output3 = df + fill_na("bfill")
    expected3 = pandas_df.fillna("bfill")
    pd.testing.assert_frame_equal(output3, expected3)

    output4 = df + fill_na("pad")
    expected4 = pandas_df.fillna("pad")
    pd.testing.assert_frame_equal(output4, expected4)

    output5 = df + fill_na("ffill")
    expected5 = pandas_df.fillna("ffill")
    pd.testing.assert_frame_equal(output5, expected5)

    output6 = df + fill_na(method="ffill", axis=1)
    expected6 = pandas_df.fillna(method="ffill", axis=1)
    pd.testing.assert_frame_equal(output6, expected6)

    output7 = df + fill_na(method="ffill", limit=1)
    expected7 = pandas_df.fillna(method="ffill", limit=1)
    pd.testing.assert_frame_equal(output7, expected7)

    output8 = df + fill_na(value=0, limit=1)
    expected8 = pandas_df.fillna(value=0, limit=1)
    pd.testing.assert_frame_equal(output8, expected8)

    d = {"col1": 9, "col2": 10, "col3": 11, "col4": 12}
    output9 = df + fill_na(value=d)
    expected9 = pandas_df.fillna(value=d)
    pd.testing.assert_frame_equal(output9, expected9)

    new_df = pd.DataFrame(np.ones((4, 4)), columns=["col1", "col2", "A", "B"])
    output10 = df + fill_na(new_df)
    expected10 = pandas_df.fillna(new_df)
    pd.testing.assert_frame_equal(output10, expected10)

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
    with pytest.raises(IOError) as context:
        new_df = df + write_file("df.abc")


if __name__ == "__main__":
    test_drop()
    test_check_null()
    test_drop_na()
    test_fill_na()
    test_merge()
    test_write_file()
