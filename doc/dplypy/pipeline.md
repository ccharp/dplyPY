## Module dplypy.pipeline
This module is entirely composed of functions.

The literal sum (DplyFrame.\__add\__()) of these functions is a data pipeline.

### Functions
---
#### `arrange(by, axis=0, ascending=True)`
##### Description
Sort the DplyFrame and return a new DplyFrame

##### Parameters
<li> by: mapping, function, label, or list of labels
<li> axis: 0 for index, 1 for columns
<li> ascending: whether or not the data should be sorted in ascending order (False for descending).

##### Return: a function that returns a new DplyFrame
---
#### `count_null(column=None, index=None)`
##### Description
Get total number of null values in a DplyFrame, one or more rows of DplyFrame, or one or more columns of DplyFrame

##### Parameters
<li> column: one column name or one list of column names of a DplyFrame
<li> index: one row name or one list of row names of a DplyFrame

##### Return: a nonnegative integer
---
#### `drop(labels=None, axis=0, index=None, columns=None)`
##### Description
Drop rows or columns from the DplyFrame. Remove rows or columns either by giving the label names and axis, or by giving the specific index or column names

##### Parameters
<li> labels: the label names of the rows/columns to be dropped. Single or list-like
<li> axis: 0 or 'index', 1 or 'columns'
<li> index: the label names of the rows to be dropped. Single or list-like
<li> columns: the column names of the rows to be dropped. Single or list-like

##### Return: a function that returns a new DplyFrame
---
#### `drop_na(axis=0, how='any', thresh=None, subset=None)`
##### Description
Remove missing values of a DplyFrame

##### Parameters
<li> axis: drop rows (0 or "index") or columns (1 or "columns") with default value 0
<li> how: drop rows/columns with any missing value ("any") or all missing values ("all")
<li> thresh: drop rows/columns with at least thresh amount of missing values
<li> subset: drop missing values only in subset of rows/columns, where rows/columns correspond to the other axis

##### Return: a function that returns a new DplyFrame
---
#### `fill_na(value=None, method=None, axis=0, limit=None)`
##### Description
Fill missing values in a DplyFrame with value

##### Parameters
<li> value: used for filling missing values, can be scaler, dict, series, or dataframe, must be None when method is not None
<li> method: use "pad" or "ffill" to propagate last valid observation forward, and use "backfill" or "bfill" to use next valid observation to fill gap
<li> axis: along 0/"index" or 1/"columns" to fill missing values with default value 0
<li> limit: must be positive if not None. If method is not None, it is the maximum consecutive missing values to fill; otherwise, it fills at most limit number of missing values along the entire axis
    
##### Return: a function that returns a new DplyFrame
---
#### `filter(boolean_series)`
##### Description
Cull rows by boolean series and return a new DplyFrame. 

Works similarly to DplyFrame.\__get\__()
    
##### Parameters
<li> boolean_series: must have the same number of rows as the incoming DplyFrame. Removes rows corresponding to False values in the series
    
##### Return: a function that returns a new DplyFrame
---
#### `gather(id_vars=None, value_vars=None, var_name=None, value_name='value', ignore_index=True)`
##### Description
Unpivot a DplyFrame from wide to long format.

##### Parameters
<li> id_vars: Column(s) being used as identifier variables. Tuple, list or ndarray
<li> value_vars: Columns to unpivot. Default to be all columns not in id_vars. Tuple, list or ndarray
<li> var_name: Name of the variable column
<li> value_name: Name of the value column
<li> ignore_index: Original index would be ignored if True; else otherwise

##### Return: a function that returns a new DplyFrame
---
#### `head(n)`
##### Description
Returns the first n rows of a DplyFrame

##### Parameters
<li> n: number of rows to select

##### Return: a function that returns a new DplyFrame
---
#### `join(right: dplypy.dplyframe.DplyFrame, how='inner', on=None, left_on=None, right_on=None, left_index=False, right_index=False, sort=False, suffixes=('_x', '_y'))`
##### Description
Combines two DplyFrames together based on a join key. 

Functions like a SQL join.

##### Parameters
<li> right: the other DplyFrame to be merged against
<li> how: accepts {‘left’, ‘right’, ‘outer’, ‘inner’, ‘cross’}, default ‘inner’
<li> on: column or index to join on. Must exist in both DplyFrames
<li> left_on: column or index in the left frame to join on
<li> right_on: column or index in the left frame to join on
<li> left_index: index of the left DplyFrame to be used as the join key
<li> right_index: index of the right DplyFrame to be used as the join key
<li> sort: if true, sort the keys in lexigraphical order
<li> suffixes: suffix to be applied to variables in left and right DplyFrames

##### Return: a function that returns a new DplyFrame
---
#### `mutate(func, axis=0)`
##### Description
Apply a function along an axis of the DplyFrame
    
##### Parameters
<li> func: the function to appply to each column or row
<li> axis: specifies the axis to which `func` is applied, 0 for 'index', 1 for 'columns'

##### Return: a function that returns a new DplyFrame
---
#### `one_hot(prefix=None, prefix_sep='_', dummy_na=False, columns=None, drop_first=False, dtype=numpy.uint8)`
##### Description
Convert categorical variables to indicators and return a new DplyFrame

##### Parameters
<li> prefix: single string or list or dictionary of string to be placed before column names
<li> prefix_sep: separator between prefix and column name
<li> dummy_na: if adding a column for missing values
<li> columns: column names being encoded with default None, i.e. considering everything
<li> drop_first: if removing first indicator column
<li> dtype: only one type for new columns with default unsigned 8-bit integer

##### Return: a function that returns a new DplyFrame
---
#### `pivot_table(values=None, index=None, columns=None, aggfunc='mean', fill_value=None, dropna=True)`
##### Description
Create a spreadsheet style pivot table as a DplyFrame

##### Parameters
<li> values: columns to be aggregated
<li> index: keys to group by on the pivot table index
<li> columns: keys to group by on the pivot table column
<li> aggfunc: aggregation functions
<li> fill_value: the value to replace the nan values in the table after aggregation
<li> whether to drop the columns with all nan values

##### Return: a function that returns a new DplyFrame
---   
#### `row_name_subset(arg)`
##### Description
Slice a DplyFrame access rows by index names and return a new DplyFrame

##### Parameters
<li> args: a list or array of row names, an alignable boolean series, or an alignable index

##### Return: a function that returns a new DplyFrame
---
#### `s(side_effect_func: Callable[[dplypy.dplyframe.DplyFrame], None]) ‑> Callable[[dplypy.dplyframe.DplyFrame], dplypy.dplyframe.DplyFrame]`
##### Description
Convenience method for `side_effect()`. See `side_effect` for operational details.

---
#### `select(query_str: str)`
##### Description
Query the columns of a DplyFrame with a boolean expression

##### Parameters
<li> query_str: string representation of the query, e.g. 'A > B'

##### Return: a function that returns a new DplyFrame
--- 
#### `side_effect(side_effect_func: Callable[[dplypy.dplyframe.DplyFrame], None]) ‑> Callable[[dplypy.dplyframe.DplyFrame], dplypy.dplyframe.DplyFrame]`
##### Description
Allows user to inject arbitrary side effects into the pipeline, e.g. render a plot or do network operation.

Note that the input function receives a copy of the data frame, i.e. modifications will not be preserved.
    
##### Parameters
<li> side_effect_func: performs the side effect

##### Return: a function that returns the original DplyFrame
---
#### `slice_column(*args)`
##### Description
Purely integer-location based indexing for column selection by position.

##### Parameters
<li> args: a list or array of integers, a boolean array, a callable function, or two integers start and end indices

##### Return: a function that returns a new DplyFrame
---
#### `slice_row(*args)`
##### Description
Purely integer-location based indexing for row selection by position.
   
##### Parameters
<li> args: a list or array of integers, a boolean array, a callable function, or two integers start and end indices

##### Return: a function that returns a new DplyFrame
---
#### `tail(n)`
##### Description
Returns the last n rows of a DplyFrame

##### Parameters
<li> n: number of rows to select

##### Return: a function that returns a new DplyFrame
---
#### `write_file(file_path, sep=',', index=True)`
##### Description
Write DplyFrame to file. 

Write the DplyFrame to the following file types depending on the file path given .csv, .xlsx, .json and .pkl

##### Parameters
<li> file_path: the path of the file with proper suffix
<li> sep: the separator for csv files. Default to be comma
<li> index: Write the row name by the index of the DplyFrame. Default to be true

##### Return: a function that returns the original DplyFrame