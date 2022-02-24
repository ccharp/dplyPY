import pandas as pd
from src.dplypy import DplyFrame
from src.pipeline import query, apply, drop, write_file, check_null
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

    output1 = df1 + check_null()
    expected1 = True
    assert output1 == expected1

    output2 = df1 + check_null(column="col1")
    expected2 = True
    assert output2 == expected2

    output3 = df1 + check_null(column="col2")
    expected3 = False
    assert output3 == expected3

    output4 = df1 + check_null(column="col3")
    expected4 = True
    assert output4 == expected4

    try:
        df1 + check_null(column="col4")
    except KeyError:
        pass
    else:
        raise AssertionError("KeyError was not raised")

    output5 = df1 + check_null(choice=2)
    expected5 = 3
    assert output5 == expected5

    output6 = df1 + check_null(column="col1", choice=2)
    expected6 = 1
    assert output6 == expected6

    output7 = df1 + check_null(column="col2", choice=2)
    expected7 = 0
    assert output7 == expected7

    output8 = df1 + check_null(column="col3", choice=2)
    expected8 = 2
    assert output8 == expected8

    try:
        df1 + check_null(column="col4", choice=2)
    except KeyError:
        pass
    else:
        raise AssertionError("KeyError was not raised")

    try:
        df1 + check_null(choice=3)
    except ValueError:
        pass
    else:
        raise AssertionError("ValueError was not raised")

    pandas_df2 = pd.DataFrame(data={"col5": [1, 2, 3, 4], "col6": [5, 6, 7, 8]})

    df2 = DplyFrame(pandas_df2)

    output9 = df2 + check_null()
    expected9 = False
    assert output9 == expected9

    output10 = df2 + check_null(column="col5")
    expected10 = False
    assert output10 == expected10

    output11 = df2 + check_null(choice=2)
    expected11 = 0
    assert output11 == expected11

    output12 = df2 + check_null(column="col5", choice=2)
    expected12 = 0
    assert output12 == expected12


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
    test_write_file()
