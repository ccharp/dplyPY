import pandas as pd
from dplypy import DplyFrame
from pipeline import query, apply

# TODO: idomatic python testing
#       which testing framework should we use?

def _test():
    pandas_df = pd.DataFrame(np.array(([1, 2, 3], [1, 5, 6], [6, 7, 8])),
                          index=['mouse', 'rabbit', 'owl'],
                          columns=['col1', 'col2', 'col3'])

    d = DplyFrame(pandas_df)
    output = d + apply(lambda x: x + 1) + query('col1 == 2')
    print(output.pandas_df)


if __name__ == '__main__':
    _test()