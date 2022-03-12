import pandas as pd

from dplypy.dplypy import DplyFrame, merge


def test_merge():
    df_l = DplyFrame(
        pd.DataFrame(
            data={
                "common": [1, 2, 3, 4],
                "left_index": ["a", "b", "c", "d"],
                "left_key": [3, 4, 7, 6],
                "col3": [6, 7, 8, 9],
                "col4": [9, 10, 11, 12],
            }
        )
    )
    df_r = DplyFrame(
        pd.DataFrame(
            data={
                "common": [1, 2, 3, 4],
                "right_index": ["a", "b", "foo", "d"],
                "right_key": [3, 4, 5, 6],
                "col3": [1, 2, 3, 4],
                "col4": [5, 6, 7, 8],
            }
        )
    )

    # on
    output1 = df_l + merge(df_r, on="common")
    expected1 = df_l.pandas_df.merge(df_r.pandas_df, on="common")
    pd.testing.assert_frame_equal(output1.pandas_df, expected1)

    # sort
    output2 = df_l + merge(df_r, on="common", sort=True)
    expected2 = df_l.pandas_df.merge(df_r.pandas_df, on="common", sort=True)
    pd.testing.assert_frame_equal(output2.pandas_df, expected2)

    # left_on, right_on, suffixes
    output3 = df_l + merge(
        df_r, left_on="left_key", right_on="right_key", suffixes=("_foo_x", "_foo_y")
    )
    expected3 = df_l.pandas_df.merge(
        df_r.pandas_df,
        left_on="left_key",
        right_on="right_key",
        suffixes=("_foo_x", "_foo_y"),
    )
    pd.testing.assert_frame_equal(output3.pandas_df, expected3)

    try:
        df_l + merge(df_r, left_on="left_key")
    except pd.errors.MergeError:
        pass
    else:
        raise AssertionError("MergeError was not raised")

    try:
        df_l + merge(df_r, right_on="right_key")
    except pd.errors.MergeError:
        pass
    else:
        raise AssertionError("MergeError was not raised")

    try:
        df_l + merge(
            df_r, left_on="left_key", right_on="right_key", suffixes=(None, None)
        )
    except ValueError:
        pass
    else:
        raise AssertionError("ValueError was not raised")

    # left_index, right_index
    output4 = df_l + merge(df_r, left_index=True, right_index=True)
    expected4 = df_l.pandas_df.merge(
        df_r.pandas_df, left_index=True, right_index=True)
    pd.testing.assert_frame_equal(output4.pandas_df, expected4)

    output5 = df_l + merge(df_r, left_on="left_key", right_index=True)
    expected5 = df_l.pandas_df.merge(
        df_r.pandas_df, left_on="left_key", right_index=True
    )
    pd.testing.assert_frame_equal(output5.pandas_df, expected5)

    output6 = df_l + merge(df_r, left_index=True, right_on="right_key")
    expected6 = df_l.pandas_df.merge(
        df_r.pandas_df, left_index=True, right_on="right_key"
    )
    pd.testing.assert_frame_equal(output6.pandas_df, expected6)

    try:
        df_l + merge(df_r, left_index=True)
    except pd.errors.MergeError:
        pass
    else:
        raise AssertionError("MergeError was not raised")

    try:
        df_l + merge(df_r, right_index=True)
    except pd.errors.MergeError:
        pass
    else:
        raise AssertionError("MergeError was not raised")

    df_l_new = DplyFrame(
        pd.DataFrame(
            data={
                "col1": [1, 2, 3, 4],
                "col2": [9, 10, 11, 12],
                "col3": [17, 18, 19, 20],
            }
        )
    )
    df_r_new = DplyFrame(
        pd.DataFrame(
            data={"col1": [1, 2, 3, 5], "col4": [
                5, 6, 7, 8], "col5": [5, 6, 7, 8]}
        )
    )

    # how
    output7 = df_l_new + merge(df_r_new, how="left")
    expected7 = df_l_new.pandas_df.merge(df_r_new.pandas_df, how="left")
    pd.testing.assert_frame_equal(output7.pandas_df, expected7)

    output8 = df_l_new + merge(df_r_new, on="col1", how="left")
    expected8 = df_l_new.pandas_df.merge(
        df_r_new.pandas_df, on="col1", how="left")
    pd.testing.assert_frame_equal(output8.pandas_df, expected8)

    output9 = df_l_new + merge(df_r_new, how="right")
    expected9 = df_l_new.pandas_df.merge(df_r_new.pandas_df, how="right")
    pd.testing.assert_frame_equal(output9.pandas_df, expected9)

    output10 = df_l_new + merge(df_r_new, how="outer")
    expected10 = df_l_new.pandas_df.merge(df_r_new.pandas_df, how="outer")
    pd.testing.assert_frame_equal(output10.pandas_df, expected10)

    # cross (new for pandas 1.2.0)
    output11 = df_l_new + merge(df_r_new, how="cross")
    expected11 = df_l_new.pandas_df.merge(df_r_new.pandas_df, how="cross")
    pd.testing.assert_frame_equal(output11.pandas_df, expected11)

    try:
        df_l_new + merge(df_r_new, on="col1", how="cross")
    except pd.errors.MergeError:
        pass
    else:
        raise AssertionError("MergeError was not raised")

    try:
        df_l_new + merge(df_r_new, left_on="col2",
                         right_on="col4", how="cross")
    except pd.errors.MergeError:
        pass
    else:
        raise AssertionError("MergeError was not raised")

    try:
        df_l_new + merge(df_r_new, left_index=True,
                         right_index=True, how="cross")
    except pd.errors.MergeError:
        pass
    else:
        raise AssertionError("MergeError was not raised")
