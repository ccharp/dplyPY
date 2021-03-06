"""
This module is entirely composed of functions.
The literal sum (DplyFrame.__add__()) of these functions is a data pipeline.
"""
from typing import Callable
import pandas as pd
import numpy as np
from dplypy import DplyFrame


def head(n):
    """
    Returns the first n rows of a DplyFrame

    :param n: number of rows to select
    :return: a function that returns a new DplyFrame
    """
    return lambda d1: DplyFrame(d1.pandas_df.head(n))


def tail(n):
    """
    Returns the last n rows of a DplyFrame

    :param n: number of rows to select
    :return: a function that returns a new DplyFrame
    """
    return lambda d1: DplyFrame(d1.pandas_df.tail(n))


def select(query_str: str):
    """
    Query the columns of a DplyFrame with a boolean expression

    :param query_str: string representation of the query, e.g. 'A > B'
    :return: a function that returns a new DplyFrame
    """
    return lambda d1: DplyFrame(d1.pandas_df.query(query_str))


def mutate(func, axis=0):
    """
    Apply a function along an axis of the DplyFrame

    :param func: the function to appply to each column or row
    :param axis: specifies the axis to which `func` is applied, 0 for 'index', 1 for 'columns'
    :return: a function that returns a new DplyFrame
    """
    return lambda d1: DplyFrame(d1.pandas_df.apply(func=func, axis=axis))


def drop(labels=None, axis=0, index=None, columns=None):
    """
    Drop rows or columns from the DplyFrame
    Remove rows or columns either by giving the label names and axis,
    or by giving the specific index or column names

    :param labels: the label names of the rows/columns to be dropped. Single or list-like
    :param axis: 0 or 'index', 1 or 'columns'
    :param index: the label names of the rows to be dropped. Single or list-like
    :param columns: the column names of the rows to be dropped. Single or list-like
    :return: a function that returns a new DplyFrame
    """
    return lambda d1: DplyFrame(
        d1.pandas_df.drop(labels=labels, axis=axis, index=index, columns=columns)
    )


def count_null(column=None, index=None):
    """
    Get total number of null values in a DplyFrame,
    one or more rows of DplyFrame,
    or one or more columns of DplyFrame
    :param column: one column name or one list of column names of a DplyFrame
    :param index: one row name or one list of row names of a DplyFrame
    :return: a nonnegative integer
    """

    def _count_null(d1, column=None, index=None):
        if column is not None:
            return d1.pandas_df[column].isna().sum().sum()
        if index is not None:
            return d1.pandas_df.loc[index].isna().sum().sum()
        return d1.pandas_df.isnull().sum().sum()

    return lambda d1: _count_null(d1, column, index)


def drop_na(axis=0, how="any", thresh=None, subset=None):
    """
    Remove missing values of a DplyFrame

    :param axis: drop rows (0 or "index") or columns (1 or "columns") with default value 0
    :param how: drop rows/columns with any missing value ("any") or all missing values ("all")
    :param thresh: drop rows/columns with at least thresh amount of missing values
    :param subset: drop missing values only in subset of rows/columns,
                   where rows/columns correspond to the other axis
    :return: a function that returns a new DplyFrame
    """
    return lambda d1: DplyFrame(
        d1.pandas_df.dropna(axis=axis, how=how, thresh=thresh, subset=subset)
    )


def fill_na(value=None, method=None, axis=0, limit=None):
    """
    Fill missing values in a DplyFrame with value

    :param value: used for filling missing values,
                           can be scaler, dict, series, or dataframe,
                           must be None when method is not None
    :param method: use "pad" or "ffill" to propagate last valid observation forward,
                   and use "backfill" or "bfill" to use next valid observation to fill gap
    :param axis: along 0/"index" or 1/"columns" to fill missing values with default value 0
    :param limit: must be positive if not None.
                  If method is not None, it is the maximum consecutive missing values to fill;
                  otherwise, it fills at most limit number of missing values along the entire axis
    :return: a function that returns a new DplyFrame
    """
    return lambda d1: DplyFrame(
        d1.pandas_df.fillna(value=value, method=method, axis=axis, limit=limit)
    )


def join(
    right: DplyFrame,
    how="inner",
    on=None,
    left_on=None,
    right_on=None,
    left_index=False,
    right_index=False,
    sort=False,
    suffixes=("_x", "_y"),
):
    """
    Combines two DplyFrames together based on a join key
    Functions like a SQL join.

    :param right: the other DplyFrame to be merged against
    :param how: accepts {???left???, ???right???, ???outer???, ???inner???, ???cross???}, default ???inner???
    :param on: column or index to join on. Must exist in both DplyFrames
    :param left_on: column or index in the left frame to join on
    :param right_on: column or index in the left frame to join on
    :param left_index: index of the left DplyFrame to be used as the join key
    :param right_index: index of the right DplyFrame to be used as the join key
    :param sort: if true, sort the keys in lexigraphical order
    :param suffixes: suffix to be applied to variables in left and right DplyFrames
    :return: a function that returns a new DplyFrame
    """
    return lambda d1: DplyFrame(
        d1.pandas_df.merge(
            right.pandas_df,
            how,
            on,
            left_on,
            right_on,
            left_index,
            right_index,
            sort,
            suffixes,
        )
    )


def write_file(file_path, sep=",", index=True):
    """
    Write DplyFrame to file.
    Write the DplyFrame to the following file types depending on the file path given:
    .csv, .xlsx, .json and .pkl

    :param file_path: the path of the file with proper suffix
    :param sep: the separator for csv files. Default to be comma
    :param index: Write the row name by the index of the DplyFrame. Default to be true
    :return: a function that returns the original DplyFrame
    """

    def to_csv(d1):
        d1.pandas_df.to_csv(file_path, sep=sep, index=index)
        return d1

    def to_excel(d1):
        d1.pandas_df.to_excel(file_path, index=index)
        return d1

    def to_json(d1):
        d1.pandas_df.to_json(file_path)
        return d1

    def to_pickle(d1):
        d1.pandas_df.to_pickle(file_path)
        return d1

    if file_path.endswith(".csv"):
        return to_csv
    if file_path.endswith(".xlsx"):
        return to_excel
    if file_path.endswith(".json"):
        return to_json
    if file_path.endswith(".pkl"):
        return to_pickle
    raise IOError("The file format is not supported.")


def pivot_table(
    values=None, index=None, columns=None, aggfunc="mean", fill_value=None, dropna=True
):
    """
    Create a spreadsheet style pivot table as a DplyFrame

    :param values: columns to be aggregated
    :param index: keys to group by on the pivot table index
    :param columns: keys to group by on the pivot table column
    :param aggfunc: aggregation functions
    :param fill_value: the value to replace the nan values in the table after aggregation
    :param whether to drop the columns with all nan values
    :return: a function that returns a new DplyFrame
    """
    return lambda d1: DplyFrame(
        d1.pandas_df.pivot_table(
            values=values,
            index=index,
            columns=columns,
            aggfunc=aggfunc,
            fill_value=fill_value,
            dropna=dropna,
        )
    )


def side_effect(
    side_effect_func: Callable[[DplyFrame], None]
) -> Callable[[DplyFrame], DplyFrame]:
    """
    Allows user to inject arbitrary side effects into the pipeline,
    e.g. render a plot or do network operation.
    Note that the input function receives a copy of the data frame
    i.e. modifications will not be preserved.

    :param side_effect_func: performs the side effect
    :return: a function that returns the original DplyFrame
    """

    def d2_func(d1: DplyFrame):
        """
        The deep copy prevents side_effect_func from modifying the Data Frame,
        in a way that is preserved down the pipeline
        """
        side_effect_func(d1.deep_copy())
        return d1

    return d2_func


def s(
    side_effect_func: Callable[[DplyFrame], None]
) -> Callable[[DplyFrame], DplyFrame]:
    """
    Convenience method for `side_effect()`. See `side_effect` for operational details.
    """
    return side_effect(side_effect_func)


def gather(
    id_vars=None, value_vars=None, var_name=None, value_name="value", ignore_index=True
):
    """
    Unpivot a DplyFrame from wide to long format.

    :param id_vars: Column(s) being used as identifier variables. Tuple, list or ndarray
    :param value_vars: Columns to unpivot. Default to be all columns not in id_vars.
                       Tuple, list or ndarray
    :param var_name: Name of the variable column
    :param value_name: Name of the value column
    :param ignore_index: Original index would be ignored if True; else otherwise
    :return: a function that returns a new DplyFrame
    """
    return lambda d1: DplyFrame(
        d1.pandas_df.melt(
            id_vars=id_vars,
            value_vars=value_vars,
            var_name=var_name,
            value_name=value_name,
            ignore_index=ignore_index,
        )
    )


def one_hot(
    prefix=None,
    prefix_sep="_",
    dummy_na=False,
    columns=None,
    drop_first=False,
    dtype=np.uint8,
):
    """
    Convert categorical variables to indicators and return a new DplyFrame

    :param prefix: single string or list or dictionary of string to be placed before column names
    :param prefix_sep: separator between prefix and column name
    :param dummy_na: if adding a column for missing values
    :param columns: column names being encoded with default None, i.e. considering everything
    :param drop_first: if removing first indicator column
    :param dtype: only one type for new columns with default unsigned 8-bit integer
    :return: a function that returns a new DplyFrame
    """
    return lambda d1: DplyFrame(
        pd.get_dummies(
            d1.pandas_df,
            prefix=prefix,
            prefix_sep=prefix_sep,
            dummy_na=dummy_na,
            columns=columns,
            drop_first=drop_first,
            dtype=dtype,
        )
    )


def filter(boolean_series):
    """
    Cull rows by boolean series and return a new DplyFrame. Works similarly to DplyFrame.__get__()

    :param boolean_series: must have the same number of rows as the incoming DplyFrame.
                           Removes rows corresponding to False values in the series
    :return: a function that returns a new DplyFrame
    """
    return lambda d1: DplyFrame(d1[boolean_series])


def arrange(by, axis=0, ascending=True):
    """
    Sort the DplyFrame and return a new DplyFrame

    :param by: mapping, function, label, or list of labels
    :param axis: 0 for index, 1 for columns
    :param ascending: whether or not the data should be sorted in ascending order
                      (False for descending)
    :return: a function that returns a new DplyFrame
    """
    return lambda d1: DplyFrame(
        d1.pandas_df.sort_values(by=by, axis=axis, ascending=ascending)
    )


def row_name_subset(arg):
    """
    Slice a DplyFrame access rows by index names and return a new DplyFrame

    :param args: a list or array of row names,
                 an alignable boolean series,
                 or an alignable index
    :return: a function that returns a new DplyFrame
    """
    return lambda d1: DplyFrame(d1.pandas_df.loc[arg])


def slice_row(*args):
    """
    Purely integer-location based indexing for row selection by position.

    :param args: a list or array of integers,
                 a boolean array, a callable function,
                 or two integers start and end indices
    :return: a function that returns a new DplyFrame
    """
    if len(args) == 2:
        return lambda d1: DplyFrame(d1.pandas_df.iloc[args[0] : args[1]])
    return lambda d1: DplyFrame(d1.pandas_df.iloc[args[0]])


def slice_column(*args):
    """
    Purely integer-location based indexing for column selection by position.

    :param args: a list or array of integers,
                 a boolean array, a callable function,
                 or two integers start and end indices
    :return: a function that returns a new DplyFrame
    """
    if len(args) == 2:
        return lambda d1: DplyFrame(d1.pandas_df.iloc[:, args[0] : args[1]])
    return lambda d1: DplyFrame(d1.pandas_df.iloc[:, args[0]])
