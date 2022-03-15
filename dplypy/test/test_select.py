import pandas as pd
import numpy as np

from dplypy.dplyframe import DplyFrame
from dplypy.pipeline import select


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
