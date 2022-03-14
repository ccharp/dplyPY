##Software Design
The overall design is quite simple: we have a single class representing a dataframe and a module containing all pipeline methods. 

###Components

####Package: `dplypy`
Since the two modules, dplyframe and pipeline rely on each other to provide the combined functionality of Dplyr, the package is imported with a single statement, that is, they cannot be imported separately. 

#####Module: `dplyframe.py`
DplyFrame represents the data we want to transform. The DplyFrame is water that flows through the pipeline. Behaviorally, it almost entirely mirrors `Pandas.DataFrame`, excepting the (`+`) operator which allows pipeline methods to be chained together. 

######Module: `pipeline.py`
This module is entirely composed of functions. The literal sum (`DplyFrame.__add__()`) of these functions is a data pipeline. Each function takes a `DplyFrame` and returns a function that returns a DplyFrame (lazy evaluation). 
