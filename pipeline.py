from dplypy import DplyFrame

def query(query_str: str):
    return lambda d1: DplyFrame(d1.pandas_df.query(query_str))

def apply(func, axis=0, **kwargs): # TODO: CSC: use kwargs... couldn't quickly figure out how to do it
    return lambda d1: DplyFrame(d1.pandas_df.apply(func=func, axis=axis)) 