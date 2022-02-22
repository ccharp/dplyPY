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
        print("Unfortunately we do not support this file type. Please write the files with the following types: \n")
        print(".csv\t.json\t.pkl\t.xlsx")

    return lambda d1: DplyFrame(
        d1.pandas_df.drop(labels=labels, axis=axis, index=index, columns=columns)
    )
