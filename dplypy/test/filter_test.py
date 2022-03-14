import pandas as pd
import numpy as np

from dplypy.dplypy import DplyFrame
from dplypy.pipeline import filter


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
