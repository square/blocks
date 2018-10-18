#!/usr/bin/env python
# -*- coding: utf-8 -*-

from setuptools import setup, find_packages

with open('README.md') as readme_file:
    readme = readme_file.read()

requirements = [
    'pandas',
]

extras_require = {
    'pq': ['pyarrow'],
    'avro': ['fastavro'],
    'dev': ['pytest', 'pytest-cov', 'flake8', 'sphinx', 'numpydoc', 'sphinx-rtd-theme'],
}

setup(
    name='blocks',
    version='0.4.3',
    description='Merge dataframe blocks distributed on disk.',
    long_description=readme,
    author='Bradley Axen',
    author_email='baxen@squareup.com',
    packages=find_packages(),
    include_package_data=True,
    install_requires=requirements,
    extras_require=extras_require,
    zip_safe=False,
    keywords='blocks',
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Intended Audience :: Developers',
        'Natural Language :: English',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.7',
    ],
)
