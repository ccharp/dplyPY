import pandas as pd
from src.dplypy import DplyFrame
from src.pipeline import query, apply, drop, merge, write_file, pivot_table
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
    expected3 = df_l.pandas_df.merge(
        df_r.pandas_df, left_index=True, right_index=True)
    pd.testing.assert_frame_equal(output3.pandas_df, expected3)


def test_write_file():
    pandas_df = pd.DataFrame(data={
        'col1': [0, 1, 2, 3],
        'col2': [3, 4, 5, 6],
        'col3': [6, 7, 8, 9],
        'col4': [9, 10, 11, 12]
    })

    df = DplyFrame(pandas_df)

    # To csv file
    df + write_file('df_no_index.csv', sep=',', index=False) + \
        write_file('df_with_index.csv', sep=',', index=True)
    read_df = pd.read_csv('df_no_index.csv', sep=',')
    pd.testing.assert_frame_equal(df.pandas_df, read_df)    # Without index
    os.remove('df_no_index.csv')

    read_df = pd.read_csv('df_with_index.csv', sep=',', index_col=0)
    pd.testing.assert_frame_equal(df.pandas_df, read_df)    # With index
    os.remove('df_with_index.csv')

    # To excel file
    # Requires dependency openpyxl
    df + write_file('df_no_index.xlsx', index=False) + \
        write_file('df_with_index.xlsx', index=True)
    read_df = pd.read_excel('df_no_index.xlsx', engine='openpyxl')
    pd.testing.assert_frame_equal(df.pandas_df, read_df)    # Without index
    os.remove('df_no_index.xlsx')

    read_df = pd.read_excel('df_with_index.xlsx',
                            index_col=0, engine='openpyxl')
    pd.testing.assert_frame_equal(df.pandas_df, read_df)    # Without index
    os.remove('df_with_index.xlsx')

    # To json
    df + write_file('df_no_index.json')
    read_df = pd.read_json('df_no_index.json')
    pd.testing.assert_frame_equal(df.pandas_df, read_df)    # Without index
    os.remove('df_no_index.json')

    # To pickle
    df + write_file('df_no_index.pkl')
    read_df = pd.read_pickle('df_no_index.pkl')
    pd.testing.assert_frame_equal(df.pandas_df, read_df)    # Without index
    os.remove('df_no_index.pkl')

    # Error case
    with pytest.raises(IOError) as context:
        new_df = df + write_file('df.abc')


def test_pivot_table():
    pandas_df = pd.DataFrame({"A": ["foo", "foo", "foo", "foo", "foo",
                                    "bar", "bar", "bar", "bar"],
                              "B": ["one", "one", "one", "two", "two",
                                    "one", "one", "two", "two"],
                              "C": ["small", "large", "large", "small",
                                    "small", "large", "small", "small",
                                    "large"],
                              "D": [1, 2, 2, 3, 3, 4, 5, 6, 7],
                              "E": [2, 4, 5, 5, 6, 6, 8, 9, 9],
                              "F": [np.nan for i in range(9)]})

    df = DplyFrame(pandas_df)

    # General test
    output = df + pivot_table(values='D',
                              index=['A', 'B'], columns=['C'], aggfunc=np.sum, fill_value=0)
    expected = pandas_df.pivot_table(values='D', index=['A', 'B'], columns=['C'], aggfunc=np.sum, fill_value=0)
    pd.testing.assert_frame_equal(output, expected)

    # Testing dropna
    output = df + pivot_table(values='D',
                              index=['A', 'B'], columns=['C'], aggfunc=np.sum, dropna=False)
    expected = pandas_df.pivot_table(values='D', index=['A', 'B'], columns=['C'], aggfunc=np.sum, dropna=False)
    pd.testing.assert_frame_equal(output, expected)

    # Testing aggregation functions
    output = df + pivot_table(values='D',
                              index=['A', 'B'], columns=['C'], aggfunc=np.average)
    expected = pandas_df.pivot_table(values='D', index=['A', 'B'], columns=['C'], aggfunc=np.average)
    pd.testing.assert_frame_equal(output, expected)

    output = df + pivot_table(values='D',
                              index=['A', 'B'], columns=['C'], aggfunc=np.std)
    expected = pandas_df.pivot_table(values='D', index=['A', 'B'], columns=['C'], aggfunc=np.std)
    pd.testing.assert_frame_equal(output, expected)

    output = df + pivot_table(values='D',
                              index=['A', 'B'], columns=['C'], aggfunc=np.argmax)
    expected = pandas_df.pivot_table(values='D', index=['A', 'B'], columns=['C'], aggfunc=np.argmax)
    pd.testing.assert_frame_equal(output, expected)

    output = df + pivot_table(values='D',
                              index=['A', 'B'], columns=['C'], aggfunc=np.argmin)
    expected = pandas_df.pivot_table(values='D', index=['A', 'B'], columns=['C'], aggfunc=np.argmin)
    pd.testing.assert_frame_equal(output, expected)
    

if __name__ == '__main__':
    test_drop()
    test_merge()
    test_write_file()
