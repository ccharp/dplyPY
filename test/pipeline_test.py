import pandas as pd
from src.dplypy import DplyFrame
from src.pipeline import query, apply, drop, write_file
import numpy as np
import os


def _test(): # TODO: convert to pytest 
    pandas_df = pd.DataFrame(np.array(([1, 2, 3], [1, 5, 6], [6, 7, 8])),
                             index=['mouse', 'rabbit', 'owl'],
                             columns=['col1', 'col2', 'col3'])

    d = DplyFrame(pandas_df)
    output = d + apply(lambda x: x + 1) + query('col1 == 2')
    print(output.pandas_df)


def test_drop():
    pandas_df = pd.DataFrame(data={
        'col1': [0, 1, 2, 3],
        'col2': [3, 4, 5, 6],
        'col3': [6, 7, 8, 9],
        'col4': [9, 10, 11, 12]
    })

    # Drop by columns
    df1 = DplyFrame(pandas_df)
    output1 = df1 + drop(['col3', 'col4'], axis=1)
    expected1 = pandas_df.drop(['col3', 'col4'], axis=1)
    pd.testing.assert_frame_equal(output1.pandas_df, expected1)

    df2 = DplyFrame(pandas_df)
    output2 = df2 + drop(columns=['col3', 'col4'])
    expected1 = pandas_df.drop(columns=['col3', 'col4'])
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


def test_write_file():
    pandas_df = pd.DataFrame(data={
        'col1': [0, 1, 2, 3],
        'col2': [3, 4, 5, 6],
        'col3': [6, 7, 8, 9],
        'col4': [9, 10, 11, 12]
    })

    df = DplyFrame(pandas_df)

    # To csv file
    df + write_file('df_no_index.csv', sep=',', index=False) + write_file('df_with_index.csv', sep=',', index=True)
    read_df = pd.read_csv('df_no_index.csv', sep=',')
    pd.testing.assert_frame_equal(df.pandas_df, read_df)    # Without index
    os.remove('df_no_index.csv')

    read_df = pd.read_csv('df_with_index.csv', sep=',', index_col=0)
    pd.testing.assert_frame_equal(df.pandas_df, read_df)    # With index
    os.remove('df_with_index.csv')

    # To excel file
    # Requires dependency openpyxl
    df + write_file('df_no_index.xlsx', index=False) + write_file('df_with_index.xlsx', index=True)
    read_df = pd.read_excel('df_no_index.xlsx', engine='openpyxl')
    pd.testing.assert_frame_equal(df.pandas_df, read_df)    # Without index
    os.remove('df_no_index.xlsx')

    read_df = pd.read_excel('df_with_index.xlsx', index_col=0, engine='openpyxl')
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
    new_df = df + write_file('df.abc')
    
    

if __name__ == '__main__':
    test_drop()
