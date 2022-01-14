#! /bin/bash

set -e

# Since extractor-utils is already a proper package (with an __init__ file),
# the import system will not recognize the rest extension as a sub-package
# unless it's in the same folder as the parent package. This is not how poetry 
# 'installs' the self-package on `poetry install`, so to work around this we
# temporarily install the package properly, and uninstall it after to avoid 
# stale copies ruining future runs.
poetry run pip install .
poetry run pytest -v
poetry run pip uninstall -y cognite-extractor-utils-rest
