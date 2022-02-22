from src.dplypy import DplyFrame


def query(
    query_str: str,
):  # summary_type: e.g. mean, variance, other descriptive stats, num rows,
    return lambda d1: DplyFrame(d1.pandas_df.query(query_str))


def apply(
    func, axis=0, **kwargs
):  # TODO: CSC: use kwargs... couldn't quickly figure out how to do it
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
