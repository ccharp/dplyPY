# DplyPy
## Getting Started
Before you commit any changes, DO THIS:
(with this repository as the CWD...)
```
pip install pre-commit
pre-commit install
```
## Building and uploading the package
(For fully contextualized instructurs ollow the instructions here: https://packaging.python.org/en/latest/tutorials/packaging-projects/
The necessary configuation files are already present.)

Run the following commands in the root repository directory:
```
python3 -m pip install --upgrade build
python3 -m build

```

The following output will be generated:
```
dist/
  example-package-YOUR-USERNAME-HERE-0.0.1-py3-none-any.whl
  example-package-YOUR-USERNAME-HERE-0.0.1.tar.gz
```

To upload the package:
```
python3 -m pip install --upgrade twine
python3 -m twine upload --repository testpypi dist/*
```
