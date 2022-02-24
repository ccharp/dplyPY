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

def check_null(column=None, choice=1):
    """
    Check null values in a dataframe or a series in a dataframe if column is provided properly:
    1. Check if there is any null value in a dataframe / series
    2. Check the total number of null values in a dataframe / series
    Return: True/False for check any. and numeric value for check total.
    :param column: one potential column name of a dataframe
    :param choice: 1 means check any, 2 means check total, otherwise raise an error
    """

    def total_null(d1, column=None):
        if column is not None:
            return d1.pandas_df[column].isna().sum()
        else:
            return d1.pandas_df.isnull().sum().sum()

    def any_null(d1, column=None):
        return total_null(d1, column) > 0

    def not_found():
        raise KeyError("Column name not found")

    def wrong_choice():
        raise ValueError("Not supported choice input")

    if choice == 1:
        return (
            lambda d1: any_null(d1, column)
            if ((column is None) or (column in d1.pandas_df))
            else not_found()
        )
    elif choice == 2:
        return (
            lambda d1: total_null(d1, column)
            if ((column is None) or (column in d1.pandas_df))
            else not_found()
        )
    else:
        return lambda d1: wrong_choice()


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
      
    if file_path.endswith(".csv"):
        return to_csv
    elif file_path.endswith(".xlsx"):
        return to_excel
    elif file_path.endswith(".json"):
        return to_json
    elif file_path.endswith(".pkl"):
        return to_pickle
    else:
        raise IOError("The file format is not supported.")

