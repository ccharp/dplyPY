# DplyPy
![Build](https://github.com/ccharp/dplyPY/actions/workflows/python-package.yml/badge.svg)


The well-known R package, dplyr, offers convenient pipelining methods for data transformations. Data pipelines like this mesh well with the functional programming style R implements: the data transformation becomes a simple chain of functions. This makes compositions of transformations as simple as composing functions. Contrasted against Pandas' OOP-style interface, dplyr pipelines are typically easier to read, understand, and implement.

We bring this functional API for transformations to Python. 

## Getting Started

### Example
```
df = dp.DplyFrame(sns.load_dataset("titanic"))

output_df = (
    df 
    + dp.gather(id_vars=["who"], value_vars=["fare"])
    + s(lambda d: print(d + head())
    + dp.pivot_table(index=["who"], values=["value"], aggfunc=min)
    + write_file("output.csv")
)
```

For a full list of available pipeline methods, see [the API docs](doc/dplypy/index.md)
### Repository Structure
```
├── LICENSE
├── README.md
├── doc
│   ├── design_spec.md
│   ├── dplypy_pres.pdf
│   ├── pipeline.py
│   └── dplypy
|       ├── dplyframe.md
│       ├── index.md
│       └── pipeline.md
├── dplypy
│   ├── __init__.py
│   ├── dplyframe.py
│   ├── pipeline.py
│   └── test
│       ├── __init__.py
│       ├── test_arrange.py
│       ├── test_count_null.py
│       ├── test_dplypy.py
│       ├── test_drop_na.py
│       ├── test_drop.py
│       ├── test_fill_na.py
│       ├── test_filter.py
│       ├── test_gather.py
│       ├── test_head.py
│       ├── test_integration.py
│       ├── test_join.py
│       ├── test_mutate.py
│       ├── test_one_hot.py
│       ├── test_pivot_table.py
│       ├── test_select.py
│       ├── test_side_effect.py
│       ├── test_slice.py
│       ├── test_tail.py
│       └── test_write_file.py
├── example
│   └── example.ipynb
├── pylintrc
└── setup.py
```

## Development
### Pre-commit hook

Automatically reformat code to comply with PEP8 standard at each commit: run the following commands:
```
# (with this repository as the CWD...)
pip install pre-commit
pre-commit install
```

### Building and uploading the package
(For fully contextualized instructurs ollow the instructions here: https://packaging.python.org/en/latest/tutorials/packaging-projects/
The necessary configuation files are already present.)

Run the following commands in the root repository directory:
```
python3 -m pip install --upgrade build
python3 -m build

```

### Continuous Integration
This project uses Github Actions to run tests. PRs that are untested or fails to build will not be merged.  
