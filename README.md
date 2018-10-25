# Blocks

Blocks provides a simple interface to read, organize, and manipulate structured data in files
on local and cloud storage. See the [documentation](https://sq-blocks.readthedocs.io) for more 
information.

    pip install sq-blocks

![blocks](docs/blocks.gif)

## Development

### Setup

For development, we use `pipenv` to manage the testing environment

To install all dependencies for local development and testing, you can do

    pipenv install

### Tests

There are two categories of tests

* `py.test` which tests that your code does what you expect
* `flake8` which verifies that you're using standard conventions in writing your code

To run them locally:

    pipenv run flake8 .
    pipenv run pytest

### Continuous Integrations

coming soon

### Versions and Tags

Use bumpversion to update the version of the package

    bumpversion [major|minor|patch]

This will increment the version and update it both in `setup.py` and `blocks/__init__.py`.
It will also automatically commit a tag with the corresponding version. You can push this to the repo
with

    git push --tags


## License

Copyright 2018 Square, Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
