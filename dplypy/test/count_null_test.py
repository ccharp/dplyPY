import pandas as pd

from dplypy.dplyframe import DplyFrame
from dplypy.pipeline import count_null


def test_count_null():
    pandas_df1 = pd.DataFrame(
        data={"col1": [1, 2, 3, None], "col2": [1, 2, 3, 4], "col3": [None, 1, None, 2]}
    )

    df1 = DplyFrame(pandas_df1)

    # Default
    output1 = df1 + count_null()
    expected1 = 3
    assert output1 == expected1

    # count column
    output2 = df1 + count_null(column="col1")
    expected2 = 1
    assert output2 == expected2

    output3 = df1 + count_null(column="col2")
    expected3 = 0
    assert output3 == expected3

    output4 = df1 + count_null(column="col3")
    expected4 = 2
    assert output4 == expected4

    output5 = df1 + count_null(column=["col1", "col3"])
    expected5 = 3
    assert output5 == expected5

    # count index
    output6 = df1 + count_null(index=0)
    expected6 = 1
    assert output6 == expected6

    output7 = df1 + count_null(index=1)
    expected7 = 0
    assert output7 == expected7

    output8 = df1 + count_null(index=[0, 2])
    expected8 = 2
    assert output8 == expected8

    try:
        df1 + count_null(column="col4")
    except KeyError:
        pass
    else:
        raise AssertionError("KeyError was not raised")

    try:
        df1 + count_null(index=4)
    except KeyError:
        pass
    else:
        raise AssertionError("KeyError was not raised")

    # no missing value
    pandas_df2 = pd.DataFrame(data={"col5": [1, 2, 3, 4], "col6": [5, 6, 7, 8]})

    df2 = DplyFrame(pandas_df2)

    output9 = df2 + count_null()
    expected9 = 0
    assert output9 == expected9

    output10 = df2 + count_null(column="col5")
    expected10 = 0
    assert output10 == expected10

    output11 = df2 + count_null(index=0)
    expected11 = 0
    assert output11 == expected11
