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
    return lambda d1: DplyFrame(
        d1.pandas_df.drop(labels=labels, axis=axis, index=index, columns=columns)
    )
