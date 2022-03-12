import pandas as pd
import numpy as np

from dplypy.dplypy import DplyFrame, melt


def test_melt():
    pandas_df = pd.DataFrame(
        {
            "A": {0: "a", 1: "b", 2: "c"},
            "B": {0: 1, 1: 3, 2: 5},
            "C": {0: 2, 1: 4, 2: 6},
        }
    )
    df = DplyFrame(pandas_df)

    # id_vars
    melted_pandas_df_1 = pd.melt(pandas_df, id_vars="A")
    melted_df_1 = df + melt(id_vars="A")
    pd.testing.assert_frame_equal(melted_pandas_df_1, melted_df_1.pandas_df)

    melted_pandas_df_2 = pd.melt(pandas_df, id_vars=["A", "B"])
    melted_df_2 = df + melt(id_vars=["A", "B"])
    pd.testing.assert_frame_equal(melted_pandas_df_2, melted_df_2.pandas_df)

    melted_pandas_df_3 = pd.melt(pandas_df, id_vars=np.array(["A", "B"]))
    melted_df_3 = df + melt(id_vars=np.array(["A", "B"]))
    pd.testing.assert_frame_equal(melted_pandas_df_3, melted_df_3.pandas_df)

    melted_pandas_df_4 = pd.melt(pandas_df, id_vars=("A", "B"))
    melted_df_4 = df + melt(id_vars=("A", "B"))
    pd.testing.assert_frame_equal(melted_pandas_df_4, melted_df_4.pandas_df)

    try:
        df + melt(id_vars=["A", "K"])
    except KeyError:
        pass
    else:
        raise KeyError("KeyError was not raised")

    # value_vars
    melted_pandas_df_5 = pd.melt(pandas_df, value_vars="B")
    melted_df_5 = df + melt(value_vars="B")
    pd.testing.assert_frame_equal(melted_pandas_df_5, melted_df_5.pandas_df)

    # This is not advisable but should work by choosing last variable
    melted_pandas_df_6 = pd.melt(pandas_df, value_vars="K")
    melted_df_6 = df + melt(value_vars="K")
    pd.testing.assert_frame_equal(melted_pandas_df_6, melted_df_6.pandas_df)

    # In contrast, this raises an error
    try:
        df + melt(value_vars=["K"])
    except KeyError:
        pass
    else:
        raise KeyError("KeyError was not raised")

    melted_pandas_df_7 = pd.melt(pandas_df, value_vars=["A", "B"])
    melted_df_7 = df + melt(value_vars=["A", "B"])
    pd.testing.assert_frame_equal(melted_pandas_df_7, melted_df_7.pandas_df)

    melted_pandas_df_8 = pd.melt(pandas_df, value_vars=("A", "B"))
    melted_df_8 = df + melt(value_vars=("A", "B"))
    pd.testing.assert_frame_equal(melted_pandas_df_8, melted_df_8.pandas_df)

    melted_pandas_df_9 = pd.melt(pandas_df, value_vars=np.array(["A", "B"]))
    melted_df_9 = df + melt(value_vars=np.array(["A", "B"]))
    pd.testing.assert_frame_equal(melted_pandas_df_9, melted_df_9.pandas_df)

    melted_pandas_df_10 = pd.melt(pandas_df, id_vars=["A"], value_vars=["B"])
    melted_df_10 = df + melt(id_vars=["A"], value_vars=["B"])
    pd.testing.assert_frame_equal(melted_pandas_df_10, melted_df_10.pandas_df)

    # Again, not advisable but should work
    melted_pandas_df_11 = pd.melt(pandas_df, id_vars=["A"], value_vars="D")
    melted_df_11 = df + melt(id_vars=["A"], value_vars="D")
    pd.testing.assert_frame_equal(melted_pandas_df_11, melted_df_11.pandas_df)

    # var_name
    melted_pandas_df_12 = pd.melt(
        pandas_df, id_vars=["A"], value_vars=["B", "C"], var_name="myVarname"
    )
    melted_df_12 = df + \
        melt(id_vars=["A"], value_vars=["B", "C"], var_name="myVarname")
    pd.testing.assert_frame_equal(melted_pandas_df_12, melted_df_12.pandas_df)

    melted_pandas_df_13 = pd.melt(
        pandas_df, id_vars=["A"], value_vars=["B"], var_name=["myVarname"]
    )
    melted_df_13 = df + \
        melt(id_vars=["A"], value_vars=["B"], var_name=["myVarname"])
    pd.testing.assert_frame_equal(melted_pandas_df_13, melted_df_13.pandas_df)

    try:
        df + melt(id_vars=["A"], value_vars=["B"],
                  var_name=["myValname", "myValname"])
    except IndexError:
        pass
    else:
        raise IndexError("IndexError was not raised")

    # value_name
    melted_pandas_df_13 = pd.melt(
        pandas_df, id_vars=["A"], value_vars=["B", "C"], value_name="myValname"
    )
    melted_df_13 = df + melt(
        id_vars=["A"], value_vars=["B", "C"], value_name="myValname"
    )
    pd.testing.assert_frame_equal(melted_pandas_df_13, melted_df_13.pandas_df)

    melted_pandas_df_14 = pd.melt(
        pandas_df,
        id_vars=["A"],
        value_vars=["B"],
        var_name="myVarname",
        value_name="myValname",
    )
    melted_df_14 = df + melt(
        id_vars=["A"], value_vars=["B"], var_name="myVarname", value_name="myValname"
    )
    pd.testing.assert_frame_equal(melted_pandas_df_14, melted_df_14.pandas_df)

    try:
        df + melt(id_vars=["A"], value_vars=["B"], value_name=["myValname"])
    except TypeError:
        pass
    else:
        raise TypeError("TypeError was not raised")

    # ignore_index
    melted_pandas_df_15 = pd.melt(
        pandas_df,
        id_vars=["A"],
        value_vars=["B"],
        var_name="myVarname",
        value_name=1234,
        ignore_index=False,
    )
    melted_df_15 = df + melt(
        id_vars=["A"],
        value_vars=["B"],
        var_name="myVarname",
        value_name=1234,
        ignore_index=False,
    )
    pd.testing.assert_frame_equal(melted_pandas_df_15, melted_df_15.pandas_df)
