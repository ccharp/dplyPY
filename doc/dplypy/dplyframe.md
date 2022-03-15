Module dplypy.dplyframe
=======================

Classes
-------

`DplyFrame(pandas_df: pandas.core.frame.DataFrame)`
:   pandas.DataFrame wrapper for implementing DyplyR style data pipelines.
    Here, we simulate dplyr's `%>%` with `+`.
    See pipeline.py for pipeline-function-specific documentation.
    
    Create a DplyFrame from a pandas DataFrame
    :param pandas_df: the data frame we wish to wrap

    ### Methods

    `deep_copy(self)`
    :