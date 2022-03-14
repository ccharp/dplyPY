import pandas as pd

from dplypy.dplypy import head, DplyFrame


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

    output1 = df + head(3)
    expected1 = pandas_df.head(3)
    pd.testing.assert_frame_equal(output1.pandas_df, expected1)

    output2 = df + head(0)
    expected2 = pandas_df.head(0)
    pd.testing.assert_frame_equal(output2.pandas_df, expected2)

    output3 = df + head(100)
    expected3 = pandas_df.head(100)
    pd.testing.assert_frame_equal(output3.pandas_df, expected3)

    output4 = df + head(-1)
    expected4 = pandas_df.head(-1)
    pd.testing.assert_frame_equal(output4.pandas_df, expected4)

    output5 = df + head(-100)
    expected5 = pandas_df.head(-100)
    pd.testing.assert_frame_equal(output5.pandas_df, expected5)

    try:
        df + head(0.1)
    except TypeError:
        pass
    else:
        raise AssertionError("TypeError was not raised")
