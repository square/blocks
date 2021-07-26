#!/usr/bin/env python
# -*- coding: utf-8 -*-

from setuptools import setup, find_packages

with open("README.md") as readme_file:
    readme = readme_file.read()

# pin fsspec until
# https://github.com/intake/filesystem_spec/commit/ee22435bc57bd9158103415c5fc58c3cbdddebf2 released
requirements = [
    "pandas",
    "fsspec==2021.6.0",
    "gcsfs",
    "pyarrow",
    "fastavro",
    "wrapt",
]

extras_require = {
    "tests": ["pytest", "pytest-cov", "delegator.py", "flake8"],
    "doc": ["sphinx", "numpydoc", "furo", "sphinx-copybutton"],
    "format": ["pre-commit"],
}
extras_require["dev"] = set(sum(extras_require.values(), []))

setup(
    name="sq-blocks",
    version="0.9.2",
    description=(
        "Blocks provides a simple interface to read, organize, and manipulate structured data"
        " in files on local and cloud storage"
    ),
    long_description=readme,
    long_description_content_type="text/markdown",
    author="Bradley Axen",
    author_email="baxen@squareup.com",
    packages=find_packages(),
    include_package_data=True,
    install_requires=requirements,
    extras_require=extras_require,
    zip_safe=False,
    keywords="blocks",
    classifiers=[
        "Intended Audience :: Developers",
        "Natural Language :: English",
        "Programming Language :: Python :: 3",
    ],
)
