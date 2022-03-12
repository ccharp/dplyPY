import pandas as pd

from dplypy.dplypy import DplyFrame, side_effect, s

def test_side_effect(capsys):
    pandas_df = pd.DataFrame(
        data={
            "col1": ["a", "b", "c", "d"],
        }
    )
    dplyf = DplyFrame(pandas_df.copy(deep=True))

    # Verify that a side effect occured
    df_post_pipe = (
        dplyf
        + s(lambda df: print(df["col1"][1]))
        + side_effect(lambda df: print(df["col1"][2]))
    )
    expected_stdout = "b\nc\n"
    captured_stdout = capsys.readouterr().out
    assert expected_stdout == captured_stdout
    # Verify that the data frame was not modified
    pd.testing.assert_frame_equal(dplyf.pandas_df, df_post_pipe.pandas_df)

    # Verfy that side effects cannot modify the data frame
    def my_side_effect_func(d1):
        d1["new_column"] = 0

    df_post_pipe = dplyf + s(my_side_effect_func)
    pd.testing.assert_frame_equal(dplyf.pandas_df, df_post_pipe.pandas_df)