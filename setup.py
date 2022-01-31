from io import open
from os import path
from setuptools import setup
import sys

DESCRIPTION = "Delta Sharing MLFlow library"

this_directory = path.abspath(path.dirname(__file__))
with open(path.join(this_directory, 'README.md'), encoding='utf-8') as f:
    LONG_DESCRIPTION = f.read()

try:
    exec(open('delta_sharing_mlflow/version.py').read())
except IOError:
    print("Failed to load version file for packaging.",
          file=sys.stderr)
    sys.exit(-1)
VERSION = __version__

setup(
    name='delta-sharing-mlflow',
    version=VERSION,
    packages=[
        'delta_sharing_mlflow',
    ],
    python_requires='>=3.6',
    install_requires=[
        'pandas',
        'cloudpickle==1.6.0',
        'delta-sharing==0.4.0',
        'sqlparse==0.4.2',
        'mlflow-skinny==1.23.1',
    ],
    author="Vuong Nguyen, Milos Colic",
    author_email="vuong.nguyen@databricks.com, milos.colic@databricks.com",
    description=DESCRIPTION,
    long_description=LONG_DESCRIPTION,
    long_description_content_type='text/markdown',
    url='https://databricks.com/'
)
