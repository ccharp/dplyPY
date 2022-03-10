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

    # drop + fill_na + count_null
    output_3 = (
        test_df_1
        + drop(labels=["survived", "pclass"], axis=1)
        + fill_na(value={"deck": "A"})
        + count_null("deck")
    )
    expected_3 = 0
    assert output_3 == expected_3

    # query + apply
    output_4 = test_df_3 + select("age > 25") + mutate(lambda b: b + 100)
    expected_4 = test_df_3.pandas_df.query('age > 25').apply(lambda b: b + 100)
    pd.testing.assert_frame_equal(output_4.pandas_df, expected_4)

    # merge + drop_na
    output_5 = test_df_3 + merge(test_df_4, on="pclass", how="outer") + drop_na()
    expected_5 = test_df_3.pandas_df.merge(test_df_4.pandas_df, on='pclass', how='outer').dropna()
    pd.testing.assert_frame_equal(output_5.pandas_df, expected_5)


if __name__ == '__main__':
    test_pipeline()