from setuptools import setup, find_packages

setup(
    name="dplypy",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        "numpy>=1.22.0",
        "pandas>=1.4.1>=1.4.1",
        "pytest>=6.2.3",
        "seaborn>=0.11.1",
        "openpyxl>=3.0.9"
        # setuptools==52.0.0.post20210125
    ],
)
