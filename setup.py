#!/usr/bin/env python
# -*- coding: utf-8 -*-

from setuptools import setup, find_packages

with open('README.md') as readme_file:
    readme = readme_file.read()

requirements = [
    'six',
    'wrapt',
    'pandas',
    'google-cloud-storage',
]

extras_require = {
    'pq': ['pyarrow'],
    'avro': ['fastavro'],
    'tests': ['pytest', 'pytest-cov', 'delegator.py', 'flake8'],
    'doc': ['sphinx', 'numpydoc', 'sphinx-rtd-theme']
}
extras_require['dev'] = set(sum(extras_require.values(), []))

setup(
    name='sq-blocks',
    version='0.6.4',
    description=(
        'Blocks provides a simple interface to read, organize, and manipulate structured data'
        ' in files on local and cloud storage'
    ),
    long_description=readme,
    long_description_content_type="text/markdown",
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
