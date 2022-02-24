"""
The Pipeline defines functions that may be used to construct a DplyR-style pipline in Python.

A pipeline may take the form:
```
read_csv("foo.csv") + drop(['X']) + query("A" > 1337) + write_csv("transformed_foo.csv")
```

Functions defined in this module are tightly coupled with the `DplyFrame` class defined in dplypy.py (see this class for additional details). For the `+` operator to work correctly, Pipeline methods must be lazily evaluated, hence they return a function to be executed at the correct time by DplyFrame.
"""

from src.dplypy import DplyFrame


def query(query_str: str):
    """
    Query the columns of a DplyFrame with a boolean expression.

    :param query_str: string representation of the query, e.g. 'A > B'
    """
    return lambda d1: DplyFrame(d1.pandas_df.query(query_str))


def apply(func, axis=0):
    """
    Apply a function along an axis of the DplyFrame.

    :param func: the function to appply to each column or row
    :param axis: specifies the axis to which `func` is applied, 0 for 'index', 1 for 'columns'.
    """
    return lambda d1: DplyFrame(d1.pandas_df.apply(func=func, axis=axis))


def drop(labels=None, axis=0, index=None, columns=None):
    """
    Drop rows or columns from the DplyFrame.

    Remove rows or columns either by giving the label names and axis, or by giving the specific index or column names.

    :param labels: the label names of the rows/columns to be dropped. Single or list-like.
    :param axis: 0 or 'index', 1 or 'columns'.
    :param index: the label names of the rows to be dropped. Single or list-like.
    :param columns: the column names of the rows to be dropped. Single or list-like.
    """
    return lambda d1: DplyFrame(d1.pandas_df.drop(labels=labels, axis=axis, index=index, columns=columns))


def write_file(file_path, sep=',', index=True):
    """
    Write DplyFrame to file.

    Write the DplyFrame to the following file types depending on the file path given: .csv, .xlsx, .json and .pkl.

    :param file_path: the path of the file with proper suffix
    :param sep: the separator for csv files. Default to be comma
    :param index: Write the row name by the index of the DplyFrame. Default to be true.
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

    if file_path.endswith('.csv'):
        return to_csv
    elif file_path.endswith('.xlsx'):
        return to_excel
    elif file_path.endswith('.json'):
        return to_json
    elif file_path.endswith('.pkl'):
        return to_pickle
    else:
        raise IOError('The file format is not supported.')


def melt(id_vars=None, value_vars=None, var_name=None, value_name='value', ignore_index=True):
    """
    Unpivot a dataframe from wide to long format.

    :param id_vars: Column(s) being used as identifier variables. Tuple, list or ndarray.
    :param value_vars: Columns to unpivot. Default to be all columns not in id_vars. Tuple, list or ndarray.
    :param var_name: Name of the variable column. 
    :param value_name: Name of the value column.
    :param ignore_index: Original index would be ignored if True; else otherwise.
    """
    return lambda d1: d1.pandas_df.melt(id_vars=id_vars, value_vars=value_vars, var_name=var_name, value_name=value_name, ignore_index=ignore_index)
