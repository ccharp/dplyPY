from cgi import test
from dplypy.dplypy import *
import seaborn as sns
import pandas as pd
import numpy as np


def test_pipeline():
    df = DplyFrame(sns.load_dataset("titanic"))

    test_df_1 = df.head(5)
    test_df_2 = df.tail(5)
    test_df_3 = DplyFrame(
        test_df_1.pandas_df.loc[:, ["survived", "pclass", "age", "sibsp"]]
    )
    test_df_4 = DplyFrame(
        test_df_2.pandas_df.loc[:, ["survived", "pclass", "age", "sibsp"]]
    )

    # melt + pivot_table
    output_1 = (
        test_df_1
        + melt(id_vars=["who"], value_vars=["fare"])
        + pivot_table(index=["who"], values=["value"], aggfunc=min)
    )
    expected_1 = test_df_1.pandas_df.melt(id_vars=["who"], value_vars=["fare"]).pivot_table(index=["who"], values=["value"], aggfunc=min)
    pd.testing.assert_frame_equal(output_1.pandas_df, expected_1)

    
    # One_hot + write_file
    output_2 = test_df_1 + one_hot(columns=["embarked"]) + write_file("one_hot.csv")
    expected_2 = pd.get_dummies(test_df_1.pandas_df, columns=["embarked"])
    pd.testing.assert_frame_equal(output_2.pandas_df, expected_2)



if __name__ == '__main__':
    test_pipeline()