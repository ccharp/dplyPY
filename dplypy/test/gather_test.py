import pandas as pd
import numpy as np

from dplypy.dplypy import DplyFrame, gather


def test_gather():
    pandas_df = pd.DataFrame(
        {
            "A": {0: "a", 1: "b", 2: "c"},
            "B": {0: 1, 1: 3, 2: 5},
            "C": {0: 2, 1: 4, 2: 6},
        }
    )
    df = DplyFrame(pandas_df)

    # id_vars
    gathered_pandas_df_1 = pd.melt(pandas_df, id_vars="A")
    gathered_df_1 = df + gather(id_vars="A")
    pd.testing.assert_frame_equal(gathered_pandas_df_1, gathered_df_1.pandas_df)

    gathered_pandas_df_2 = pd.melt(pandas_df, id_vars=["A", "B"])
    gathered_df_2 = df + gather(id_vars=["A", "B"])
    pd.testing.assert_frame_equal(gathered_pandas_df_2, gathered_df_2.pandas_df)

    gathered_pandas_df_3 = pd.melt(pandas_df, id_vars=np.array(["A", "B"]))
    gathered_df_3 = df + gather(id_vars=np.array(["A", "B"]))
    pd.testing.assert_frame_equal(gathered_pandas_df_3, gathered_df_3.pandas_df)

    gathered_pandas_df_4 = pd.melt(pandas_df, id_vars=("A", "B"))
    gathered_df_4 = df + gather(id_vars=("A", "B"))
    pd.testing.assert_frame_equal(gathered_pandas_df_4, gathered_df_4.pandas_df)

    try:
        df + gather(id_vars=["A", "K"])
    except KeyError:
        pass
    else:
        raise KeyError("KeyError was not raised")

    # value_vars
    gathered_pandas_df_5 = pd.melt(pandas_df, value_vars="B")
    gathered_df_5 = df + gather(value_vars="B")
    pd.testing.assert_frame_equal(gathered_pandas_df_5, gathered_df_5.pandas_df)

    # This is not advisable but should work by choosing last variable
    gathered_pandas_df_6 = pd.melt(pandas_df, value_vars="K")
    gathered_df_6 = df + gather(value_vars="K")
    pd.testing.assert_frame_equal(gathered_pandas_df_6, gathered_df_6.pandas_df)

    # In contrast, this raises an error
    try:
        df + gather(value_vars=["K"])
    except KeyError:
        pass
    else:
        raise KeyError("KeyError was not raised")

    gathered_pandas_df_7 = pd.melt(pandas_df, value_vars=["A", "B"])
    gathered_df_7 = df + gather(value_vars=["A", "B"])
    pd.testing.assert_frame_equal(gathered_pandas_df_7, gathered_df_7.pandas_df)

    gathered_pandas_df_8 = pd.melt(pandas_df, value_vars=("A", "B"))
    gathered_df_8 = df + gather(value_vars=("A", "B"))
    pd.testing.assert_frame_equal(gathered_pandas_df_8, gathered_df_8.pandas_df)

    gathered_pandas_df_9 = pd.melt(pandas_df, value_vars=np.array(["A", "B"]))
    gathered_df_9 = df + gather(value_vars=np.array(["A", "B"]))
    pd.testing.assert_frame_equal(gathered_pandas_df_9, gathered_df_9.pandas_df)

    gathered_pandas_df_10 = pd.melt(pandas_df, id_vars=["A"], value_vars=["B"])
    gathered_df_10 = df + gather(id_vars=["A"], value_vars=["B"])
    pd.testing.assert_frame_equal(gathered_pandas_df_10, gathered_df_10.pandas_df)

    # Again, not advisable but should work
    gathered_pandas_df_11 = pd.melt(pandas_df, id_vars=["A"], value_vars="D")
    gathered_df_11 = df + gather(id_vars=["A"], value_vars="D")
    pd.testing.assert_frame_equal(gathered_pandas_df_11, gathered_df_11.pandas_df)

    # var_name
    gathered_pandas_df_12 = pd.melt(
        pandas_df, id_vars=["A"], value_vars=["B", "C"], var_name="myVarname"
    )
    gathered_df_12 = df + \
        gather(id_vars=["A"], value_vars=["B", "C"], var_name="myVarname")
    pd.testing.assert_frame_equal(gathered_pandas_df_12, gathered_df_12.pandas_df)

    gathered_pandas_df_13 = pd.melt(
        pandas_df, id_vars=["A"], value_vars=["B"], var_name=["myVarname"]
    )
    gathered_df_13 = df + \
        gather(id_vars=["A"], value_vars=["B"], var_name=["myVarname"])
    pd.testing.assert_frame_equal(gathered_pandas_df_13, gathered_df_13.pandas_df)

    try:
        df + gather(id_vars=["A"], value_vars=["B"],
                  var_name=["myValname", "myValname"])
    except IndexError:
        pass
    else:
        raise IndexError("IndexError was not raised")

    # value_name
    gathered_pandas_df_13 = pd.melt(
        pandas_df, id_vars=["A"], value_vars=["B", "C"], value_name="myValname"
    )
    gathered_df_13 = df + gather(
        id_vars=["A"], value_vars=["B", "C"], value_name="myValname"
    )
    pd.testing.assert_frame_equal(gathered_pandas_df_13, gathered_df_13.pandas_df)

    gathered_pandas_df_14 = pd.melt(
        pandas_df,
        id_vars=["A"],
        value_vars=["B"],
        var_name="myVarname",
        value_name="myValname",
    )
    gathered_df_14 = df + gather(
        id_vars=["A"], value_vars=["B"], var_name="myVarname", value_name="myValname"
    )
    pd.testing.assert_frame_equal(gathered_pandas_df_14, gathered_df_14.pandas_df)

    try:
        df + gather(id_vars=["A"], value_vars=["B"], value_name=["myValname"])
    except TypeError:
        pass
    else:
        raise TypeError("TypeError was not raised")

    # ignore_index
    gathered_pandas_df_15 = pd.melt(
        pandas_df,
        id_vars=["A"],
        value_vars=["B"],
        var_name="myVarname",
        value_name=1234,
        ignore_index=False,
    )
    gathered_df_15 = df + gather(
        id_vars=["A"],
        value_vars=["B"],
        var_name="myVarname",
        value_name=1234,
        ignore_index=False,
    )
    pd.testing.assert_frame_equal(gathered_pandas_df_15, gathered_df_15.pandas_df)
