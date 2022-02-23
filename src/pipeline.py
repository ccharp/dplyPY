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


def merge(
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
    Combines two DplyFrames together basaed on a join key

    Functions like a SQL join.

    :param right: the other DplyFrame to be merged against
    :param how: accepts {‘left’, ‘right’, ‘outer’, ‘inner’, ‘cross’}, default ‘inner’
    :param on: column or index to join on. Must exist in both DplyFrames
    :param left_on: column or index in the left frame to join on
    :param right_on: column or index in the left frame to join on
    :param left_index: index of the left dataframe to be used as the join key
    :param right_index: index of the right dataframe to be used as the join key
    :param sort: if true, sort the keys in lexigraphical order
    :param suffixes: suffix to be applied to variables in left and right DplyFrames
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
