import pandas as pd
import numpy as np

from dplypy.dplypy import DplyFrame, one_hot


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
    expected7 = pd.get_dummies(
        pandas_df, prefix="p", prefix_sep=prefix_sep_lst)
    pd.testing.assert_frame_equal(output7.pandas_df, expected7)

    prefix_sep_dict = {"col1": "___", "col2": "__"}
    output8 = df + one_hot(prefix="p", prefix_sep=prefix_sep_dict)
    expected8 = pd.get_dummies(
        pandas_df, prefix="p", prefix_sep=prefix_sep_dict)
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
