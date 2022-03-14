import pandas as pd
import numpy as np

from dplypy.dplypy import DplyFrame
from dplypy.pipeline import pivot_table


def test_pivot_table():
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

    # General test
    output1 = df + pivot_table(index=["A", "B"], columns=["C"])
    expected1 = pandas_df.pivot_table(index=["A", "B"], columns=["C"])
    pd.testing.assert_frame_equal(output1.pandas_df, expected1)

    # values
    output2 = df + pivot_table(values="D", index=["A", "B"], columns=["C"])
    expected2 = pandas_df.pivot_table(values="D", index=["A", "B"], columns=["C"])
    pd.testing.assert_frame_equal(output2.pandas_df, expected2)

    output3 = df + pivot_table(values=["D", "F"], index=["A", "B"], columns=["C"])
    expected3 = pandas_df.pivot_table(
        values=["D", "F"], index=["A", "B"], columns=["C"]
    )
    pd.testing.assert_frame_equal(output3.pandas_df, expected3)

    # index
    output4 = df + pivot_table(index="A")
    expected4 = pandas_df.pivot_table(index="A")
    pd.testing.assert_frame_equal(output4.pandas_df, expected4)

    output5 = df + pivot_table(index=["A", "B"])
    expected5 = pandas_df.pivot_table(index=["A", "B"])
    pd.testing.assert_frame_equal(output5.pandas_df, expected5)

    output6 = df + pivot_table(index=np.arange(len(pandas_df)))
    expected6 = pandas_df.pivot_table(index=np.arange(len(pandas_df)))
    pd.testing.assert_frame_equal(output6.pandas_df, expected6)

    try:
        df + pivot_table(index=np.arange(len(pandas_df) - 1))
    except ValueError:
        pass
    else:
        raise AssertionError("ValueError was not raised")

    try:
        df + pivot_table(index=[["A"]])
    except ValueError:
        pass
    else:
        raise AssertionError("ValueError was not raised")

    try:
        df + pivot_table(values="A", index="A")
    except ValueError:
        pass
    else:
        raise AssertionError("ValueError was not raised")

    output7 = df + pivot_table(columns="C")
    expected7 = pandas_df.pivot_table(columns="C")
    pd.testing.assert_frame_equal(output7.pandas_df, expected7)

    output8 = df + pivot_table(columns=["C", "D"])
    expected8 = pandas_df.pivot_table(columns=["C", "D"])
    pd.testing.assert_frame_equal(output8.pandas_df, expected8)

    output9 = df + pivot_table(columns=np.arange(len(pandas_df)))
    expected9 = pandas_df.pivot_table(columns=np.arange(len(pandas_df)))
    pd.testing.assert_frame_equal(output9.pandas_df, expected9)

    try:
        df + pivot_table(columns=np.arange(len(pandas_df) - 1))
    except ValueError:
        pass
    else:
        raise AssertionError("ValueError was not raised")

    try:
        df + pivot_table(columns=[["A"]])
    except ValueError:
        pass
    else:
        raise AssertionError("ValueError was not raised")

    try:
        df + pivot_table(values="A", columns="A")
    except ValueError:
        pass
    else:
        raise AssertionError("ValueError was not raised")

    # fill_value
    output10 = df + pivot_table(
        values="D", index=["A", "B"], columns=["C"], fill_value=0
    )
    expected10 = pandas_df.pivot_table(
        values="D", index=["A", "B"], columns=["C"], fill_value=0
    )
    pd.testing.assert_frame_equal(output10.pandas_df, expected10)

    # Testing dropna
    output11 = df + pivot_table(
        values="D", index=["A", "B"], columns=["C"], dropna=False
    )
    expected11 = pandas_df.pivot_table(
        values="D", index=["A", "B"], columns=["C"], dropna=False
    )
    pd.testing.assert_frame_equal(output11.pandas_df, expected11)

    # Testing aggregation functions
    output12 = df + pivot_table(
        values="D", index=["A", "B"], columns=["C"], aggfunc=np.sum
    )
    expected12 = pandas_df.pivot_table(
        values="D", index=["A", "B"], columns=["C"], aggfunc=np.sum
    )
    pd.testing.assert_frame_equal(output12.pandas_df, expected12)

    output13 = df + pivot_table(
        values="D", index=["A", "B"], columns=["C"], aggfunc=np.std
    )
    expected13 = pandas_df.pivot_table(
        values="D", index=["A", "B"], columns=["C"], aggfunc=np.std
    )
    pd.testing.assert_frame_equal(output13.pandas_df, expected13)

    output14 = df + pivot_table(
        values="D", index=["A", "B"], columns=["C"], aggfunc=np.argmax
    )
    expected14 = pandas_df.pivot_table(
        values="D", index=["A", "B"], columns=["C"], aggfunc=np.argmax
    )
    pd.testing.assert_frame_equal(output14.pandas_df, expected14)

    output15 = df + pivot_table(
        values="D", index=["A", "B"], columns=["C"], aggfunc=np.argmin
    )
    expected15 = pandas_df.pivot_table(
        values="D", index=["A", "B"], columns=["C"], aggfunc=np.argmin
    )
    pd.testing.assert_frame_equal(output15.pandas_df, expected15)

    output16 = df + pivot_table(
        values="D", index=["A", "B"], columns=["C"], aggfunc=[np.argmin]
    )
    expected16 = pandas_df.pivot_table(
        values="D", index=["A", "B"], columns=["C"], aggfunc=[np.argmin]
    )
    pd.testing.assert_frame_equal(output16.pandas_df, expected16)

    output17 = df + pivot_table(
        values=["D", "E"], index=["A", "B"], columns=["C"], aggfunc=[np.argmin]
    )
    expected17 = pandas_df.pivot_table(
        values=["D", "E"], index=["A", "B"], columns=["C"], aggfunc=[np.argmin]
    )
    pd.testing.assert_frame_equal(output17.pandas_df, expected17)

    output18 = df + pivot_table(
        values=["D", "E"], index=["A", "B"], columns=["C"], aggfunc={"D": max, "E": min}
    )
    expected18 = pandas_df.pivot_table(
        values=["D", "E"], index=["A", "B"], columns=["C"], aggfunc={"D": max, "E": min}
    )
    pd.testing.assert_frame_equal(output18.pandas_df, expected18)

    output19 = df + pivot_table(
        values=["D", "E"], index=["A", "B"], columns=["C"], aggfunc={"D": max}
    )
    expected19 = pandas_df.pivot_table(
        values=["D", "E"], index=["A", "B"], columns=["C"], aggfunc={"D": max}
    )
    pd.testing.assert_frame_equal(output19.pandas_df, expected19)

    output20 = df + pivot_table(
        values=["D", "E"],
        index=["A", "B"],
        columns=["C"],
        aggfunc={"D": [max, min], "E": [max]},
    )
    expected20 = pandas_df.pivot_table(
        values=["D", "E"],
        index=["A", "B"],
        columns=["C"],
        aggfunc={"D": [max, min], "E": [max]},
    )
    pd.testing.assert_frame_equal(output20.pandas_df, expected20)

    output21 = df + pivot_table(
        values=["D", "E"], index=["A", "B"], columns=["C"], aggfunc={"D": [min, min]}
    )
    expected21 = pandas_df.pivot_table(
        values=["D", "E"], index=["A", "B"], columns=["C"], aggfunc={"D": [min, min]}
    )
    pd.testing.assert_frame_equal(output21.pandas_df, expected21)

    try:
        df + pivot_table(
            values=["D", "E"], index=["A", "B"], columns=["C"], aggfunc=[[max, min]]
        )
    except TypeError:
        pass
    else:
        raise AssertionError("TypeError was not raised")

    try:
        df + pivot_table(
            values=["D", "E"], index=["A", "B"], columns=["C"], aggfunc={"F": max}
        )
    except KeyError:
        pass
    else:
        raise AssertionError("KeyError was not raised")
