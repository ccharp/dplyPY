import pandas as pd
import functools as ft
import numpy as np

def query(query_str):
    return lambda d1: D(d1.df.query(query_str))

def apply(func, axis=0, **kwargs): # TODO: CSC: use kwargs... couldn't quickly figure out how to do it
    return lambda d1: D(d1.df.apply(func=func, axis=axis)) 

class D:
    def __init__(self, df):
        self.df = df
        self.index = df.index

    def __eq__(self, other):
        return self.df == other.df

    # TODO: implement other boolean operators

    def __getitem__(self, item):
        return self.df[item]

    def __add__(self, d2_func):
        return d2_func(self)
        
    def __repr__(self):
        self.df.to_string() # TODO: broken

def _test():
    df_old = pd.DataFrame(np.array(([1, 2, 3], [1, 5, 6], [6, 7, 8])),
                          index=['mouse', 'rabbit', 'owl'],
                          columns=['col1', 'col2', 'col3'])

    d = D(df_old)

    output = (d + apply(lambda x: x + 1)) + query('col1 == 2')
    print(output.df)

_test()