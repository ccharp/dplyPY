from dplypy import DplyFrame

def query(query_str: str): # summary_type: e.g. mean, variance, other descriptive stats, num rows, 
    return lambda d1: DplyFrame(d1.pandas_df.query(query_str))

def apply(func, axis=0, **kwargs): # TODO: CSC: use kwargs... couldn't quickly figure out how to do it
    return lambda d1: DplyFrame(d1.pandas_df.apply(func=func, axis=axis)) 

"""
TODO: 
- what functionality to we implement in addition to query and apply?
    - e.g. drop, read file, write file, plot, melt, pivot, one-hot-encoding, concat, merge, unique, check null, etc
- API for plotting intermediate transformations?
    - Summary stats at each step (mean, variance, quartiles..)
    - Preset visualization functions (scatter and line plots?)
- Fix DplyFrame print bug -- is it still an issue?
- Create a package for all this stuff
- Implement Unit tests in pytest
- API docs?  

dfply_frame + query(asdfasdf) + apply(f) + write(file.txt) + dropna() + hist(args) + query(asdfjkl) + hist(args) + summary_stats()
"""