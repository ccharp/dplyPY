import pandas as pd

from dplypy.dplyframe import DplyFrame
from dplypy.pipeline import tail


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

    output1 = df + tail(2)
    expected1 = pandas_df.tail(2)
    pd.testing.assert_frame_equal(output1.pandas_df, expected1)

    output2 = df + tail(0)
    expected2 = pandas_df.tail(0)
    pd.testing.assert_frame_equal(output2.pandas_df, expected2)

    output3 = df + tail(10)
    expected3 = pandas_df.tail(10)
    pd.testing.assert_frame_equal(output3.pandas_df, expected3)

    output4 = df + tail(-1)
    expected4 = pandas_df.tail(-1)
    pd.testing.assert_frame_equal(output4.pandas_df, expected4)

    output5 = df + tail(-10)
    expected5 = pandas_df.tail(-10)
    pd.testing.assert_frame_equal(output5.pandas_df, expected5)

    try:
        df + tail(0.1)
    except TypeError:
        pass
    else:
        raise AssertionError("TypeError was not raised")
