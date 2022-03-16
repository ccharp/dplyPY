Module dplypy.pipeline
======================

Functions
---------

    
`arrange(by, axis=0, ascending=True)`
:   Sort the DplyFrame and return a new DplyFrame
    
    :param by: mapping, function, label, or list of labels
    :param axis: 0 for index, 1 for columns
    :param ascending: whether or not the data should be sorted in ascending order
                      (False for descending)

    
`count_null(column=None, index=None)`
:   Get total number of null values in a DplyFrame,
    one or more rows of DplyFrame,
    or one or more columns of DplyFrame
    Return: a nonnegative integer
    :param column: one column name or one list of column names of a DplyFrame
    :param index: one row name or one list of row names of a DplyFrame

    
`drop(labels=None, axis=0, index=None, columns=None)`
:   Drop rows or columns from the DplyFrame and return the new DplyFrame.
    Remove rows or columns either by giving the label names and axis,
    or by giving the specific index or column names.
    
    :param labels: the label names of the rows/columns to be dropped. Single or list-like.
    :param axis: 0 or 'index', 1 or 'columns'.
    :param index: the label names of the rows to be dropped. Single or list-like.
    :param columns: the column names of the rows to be dropped. Single or list-like.

    
`drop_na(axis=0, how='any', thresh=None, subset=None)`
:   Remove missing values of a DplyFrame and return the new DplyFrame
    
    :param axis: drop rows (0 or "index") or columns (1 or "columns") with default value 0
    :param how: drop rows/columns with any missing value ("any") or all missing values ("all")
    :param thresh: drop rows/columns with at least thresh amount of missing values
    :param subset: drop missing values only in subset of rows/columns,
                   where rows/columns correspond to the other axis

    
`fill_na(value=None, method=None, axis=0, limit=None)`
:   Fill missing values in a DplyFrame with value and return the new DplyFrame
    
    :param value: used for filling missing values,
                           can be scaler, dict, series, or dataframe,
                           must be None when method is not None
    :param method: use "pad" or "ffill" to propagate last valid observation forward,
                   and use "backfill" or "bfill" to use next valid observation to fill gap
    :param axis: along 0/"index" or 1/"columns" to fill missing values with default value 0
    :param limit: must be positive if not None.
                  If method is not None, it is the maximum consecutive missing values to fill;
                  otherwise, it fills at most limit number of missing values along the entire axis

    
`filter(boolean_series)`
:   Cull rows by boolean series and return a new DplyFrame. Works similarly to DplyFrame.__get__()
    
    :param boolean_series: must have the same number of rows as the incoming DplyFrame.
                           Removes rows corresponding to False values in the series.

    
`gather(id_vars=None, value_vars=None, var_name=None, value_name='value', ignore_index=True)`
:   Unpivot a DplyFrame from wide to long format.
    
    :param id_vars: Column(s) being used as identifier variables. Tuple, list or ndarray.
    :param value_vars: Columns to unpivot. Default to be all columns not in id_vars.
                       Tuple, list or ndarray.
    :param var_name: Name of the variable column.
    :param value_name: Name of the value column.
    :param ignore_index: Original index would be ignored if True; else otherwise.

    
`head(n)`
:   Returns the first n rows of a DplyFrame
    
    :param n: number of rows to select

    
`join(right: dplypy.dplyframe.DplyFrame, how='inner', on=None, left_on=None, right_on=None, left_index=False, right_index=False, sort=False, suffixes=('_x', '_y'))`
:   Combines two DplyFrames together based on a join key and return the new DplyFrame
    Functions like a SQL join.
    
    :param right: the other DplyFrame to be merged against
    :param how: accepts {‘left’, ‘right’, ‘outer’, ‘inner’, ‘cross’}, default ‘inner’
    :param on: column or index to join on. Must exist in both DplyFrames
    :param left_on: column or index in the left frame to join on
    :param right_on: column or index in the left frame to join on
    :param left_index: index of the left DplyFrame to be used as the join key
    :param right_index: index of the right DplyFrame to be used as the join key
    :param sort: if true, sort the keys in lexigraphical order
    :param suffixes: suffix to be applied to variables in left and right DplyFrames

    
`mutate(func, axis=0)`
:   Apply a function along an axis of the DplyFrame and return the new DplyFrame
    
    :param func: the function to appply to each column or row
    :param axis: specifies the axis to which `func` is applied, 0 for 'index', 1 for 'columns'

    
`one_hot(prefix=None, prefix_sep='_', dummy_na=False, columns=None, drop_first=False, dtype=numpy.uint8)`
:   Convert categorical variables to indicators and return a new DplyFrame
    
    :param prefix: single string or list or dictionary of string to be placed before column names
    :param prefix_sep: separator between prefix and column name
    :param dummy_na: if adding a column for missing values
    :param columns: column names being encoded with default None, i.e. considering everything
    :param drop_first: if removing first indicator column
    :param dtype: only one type for new columns with default unsigned 8-bit integer

    
`pivot_table(values=None, index=None, columns=None, aggfunc='mean', fill_value=None, dropna=True)`
:   Create a spreadsheet style pivot table as a DplyFrame and return a new DplyFrame
    
    :param values: columns to be aggregated
    :param index: keys to group by on the pivot table index
    :param columns: keys to group by on the pivot table column
    :param aggfunc: aggregation functions
    :param fill_value: the value to replace the nan values in the table after aggregation
    :param whether to drop the columns with all nan values

    
`row_name_subset(arg)`
:   slice a DplyFrame access rows by index names and return a new DplyFrame
    
    :param args: a list or array of row names,
                 an alignable boolean series,
                 or an alignable index

    
`s(side_effect_func: Callable[[dplypy.dplyframe.DplyFrame], None]) ‑> Callable[[dplypy.dplyframe.DplyFrame], dplypy.dplyframe.DplyFrame]`
:   convenience method fof `side_effect()`. See `side_effect for operational details.

    
`select(query_str: str)`
:   Query the columns of a DplyFrame with a boolean expression and return the new DplyFrame
    
    :param query_str: string representation of the query, e.g. 'A > B'

    
`side_effect(side_effect_func: Callable[[dplypy.dplyframe.DplyFrame], None]) ‑> Callable[[dplypy.dplyframe.DplyFrame], dplypy.dplyframe.DplyFrame]`
:   Allows user to inject arbitrary side effects into the pipeline,
    e.g. render a plot or do network operation.
    Note that the input function receives a copy of the data frame
    i.e. modifications will not be preserved.
    
    :param side_effect_func: performs the side effect

    
`slice_column(*args)`
:   Purely integer-location based indexing for column selection by position.
    Return a new DplyFrame
    
    :param arg: a list or array of integers,
                a boolean array, a callable function,
                or two integers start and end indices

    
`slice_row(*args)`
:   Purely integer-location based indexing for row selection by position.
    Return a new DplyFrame.
    
    :param arg: a list or array of integers,
                a boolean array, a callable function,
                or two integers start and end indices

    
`tail(n)`
:   Returns the last n rows of a DplyFrame
    
    :param n: number of rows to select

    
`write_file(file_path, sep=',', index=True)`
:   Write DplyFrame to file.
    Write the DplyFrame to the following file types depending on the file path given:
    .csv, .xlsx, .json and .pkl.
    
    :param file_path: the path of the file with proper suffix
    :param sep: the separator for csv files. Default to be comma
    :param index: Write the row name by the index of the DplyFrame. Default to be true.