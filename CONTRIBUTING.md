# Contributing to Arcuate

## Overview
We happily welcome contributions to Arcuate. 
We use GitHub Issues to track community reported issues and GitHub Pull Requests for accepting changes.

## Repository structure
The repository is structured as follows:

- `arcuate/` Source code for Arcuate
- `dev/` Various scripts for developing
- `docs/` Source code for documentation
- `tests/` Unit tests for Arcuate
- `.github/workflows` CI definitions for Github Actions

## Test & build Arcuate

### Python bindings

The python bindings can be tested using [pytest](https://docs.pytest.org/).
- Install the project and its dependencies:
    `pip install -r requirements.txt && pip install -r requirements-dev.txt && pip install -e .`
- Run the tests using `python -m pytest --import-mode=append tests/`
- Run `./dev/coverage` for test coverage

The project wheel file can be built with [build](https://pypa-build.readthedocs.io/en/stable/).
- Run `./dev/build-whl`
- Collect the .whl file from `dist/`

### Documentation

The documentation has been produced using [Sphinx](https://www.sphinx-doc.org/en/master/).

To build the docs:
- Install the python requirements from `docs/docs-requirements.txt`.
- Build the HTML documentation by running `make html` from `docs/`.
- You can locally host the docs by running the `reload.py` script in the `docs/source/` directory.

## Style

We use `black` for code formatting and checking

To format your code, run `./dev/reformat`