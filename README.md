# Blocks

Blocks provides a simple interface to read, organize, and manipulate structured data in files
on local and cloud storage. See the [documentation](https://sq-blocks.readthedocs.io) for more
information.

    pip install sq-blocks

![blocks](docs/blocks.gif)

## Development

### Setup

To install all dependencies for local development and testing, you can do

    pip install -e .[dev]

### Tests

* `pytest` runs the unit tests

To run them locally:

    pytest

### Continuous Integrations

CI is handled through GitHub Actions, and will run non-GCS tests on 3.6, 3.7, 3.8.
We may add cloud storage tests to CI soon, but for now tests should also be
run locally to confirm that functionality works as well.

### Versions and Tags

Use bumpversion to update the version of the package

    bumpversion [major|minor|patch]

This will increment the version and update it both in `setup.py` and `blocks/__init__.py`.
It will also automatically commit a tag with the corresponding version. You can push this to the repo
with

    git push --tags

### Formatting

We use pre-commit to ensure consistent formatting, to make sure you run the
hooks:

    pre-commit install

### Docs

The docs are generated from the code with
[sphinx](https://www.sphinx-doc.org/en/master/), and can be tested locally:

    cd docs
    make html

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
