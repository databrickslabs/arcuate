from io import open
from os import path
from setuptools import setup
import sys

DESCRIPTION = "Delta Sharing MLFlow library"

this_directory = path.abspath(path.dirname(__file__))
with open(path.join(this_directory, "README.md"), encoding="utf-8") as f:
    LONG_DESCRIPTION = f.read()

try:
    exec(open("arcuate/version.py").read())
except IOError:
    print("Failed to load version file for packaging.", file=sys.stderr)
    sys.exit(-1)
VERSION = __version__

setup(
    name="arcuate",
    version=VERSION,
    packages=[
        "arcuate",
    ],
    python_requires=">=3.8",
    install_requires=[
        "pandas",
        "cloudpickle==1.6.0",
        "delta-sharing==0.4.0",
        "mlflow-skinny==1.23.1",
    ],
    classifiers=[
        'Programming Language :: Python :: 3',
        'License :: Other/Proprietary License',
        'Operating System :: OS Independent',
    ],
    author="Vuong Nguyen, Milos Colic",
    author_email="labs@databricks.com",
    description=DESCRIPTION,
    long_description=LONG_DESCRIPTION,
    long_description_content_type="text/markdown",
    url="https://databricks.com/learn/labs",
)
