from setuptools import setup, find_packages

with open("README.md", "r") as fh:
    long_description = fh.read()

setup(
    name = "CVM_view_market_basket",
    author="Allison Wu",
    author_email="allison.wu@thermofisher.com",
    description="market basket core algorithm and web click data processing",
    notes = "LSG view market basket",
    version = "1.0",
    packages = find_packages(),
    long_description=long_description,
    classifiers = ['Programming Language :: Python :: 3.7'],
    )
