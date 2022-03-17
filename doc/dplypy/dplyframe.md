## Module dplypy.dplyframe
DplyFrame represents the dataframe we want to transform.
### Class `DplyFrame(pandas_df: pandas.core.frame.DataFrame)`:
#### Description
`pandas.DataFrame` wrapper for implementing DplyR style data pipelines.

Here, we simulate dplyr's `%>%` with `+`.

See pipeline.py for pipeline-function-specific documentation.

#### Initialization
##### Description
Create a DplyFrame from a pandas DataFrame
##### Parameter
<li> pandas_df: the data frame we wish to wrap

#### Method
`deep_copy(self)`