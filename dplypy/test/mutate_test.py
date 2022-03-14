import pandas as pd
import numpy as np

from dplypy.dplyframe import DplyFrame
from dplypy.pipeline import mutate


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
