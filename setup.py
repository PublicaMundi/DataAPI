# -*- encoding: utf-8 -*-

import io
import os

from glob import glob
from os.path import basename
from os.path import splitext
from os.path import join
from os.path import dirname

try:
    from setuptools import setup, find_packages
except ImportError:
    from ez_setup import use_setuptools
    use_setuptools()
    from setuptools import setup, find_packages
    
def read(*names, **kwargs):
    return io.open(
        join(dirname(__file__), *names),
        encoding=kwargs.get("encoding", "utf8")
    ).read()


version = '1.0b1'


#http://stackoverflow.com/questions/14399534/how-can-i-reference-requirements-txt-for-the-install-requires-kwarg-in-setuptool
required = []
with open('requirements.txt') as f:
    required = f.read().splitlines()


setup(
    name="publicamundi-data-api",
    version=version,
    description="Application Programming Interface (API) for querying data stored by PublicaMundi extension Vector Storer",
    long_description=read("README.rst"),
    license="MIT",
    author="",
    author_email="",
    url="https://github.com/PublicaMundi/DataAPI",
    packages=find_packages("src"),
    package_dir={"": "src"},
    py_modules=[splitext(basename(path))[0] for path in glob("src/*.py")],
    include_package_data=True,
    zip_safe=False,
    classifiers=[
         # Get strings from http://pypi.python.org/pypi?%3Aaction=list_classifiers
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "Operating System :: Unix",
        "Operating System :: Microsoft :: Windows",
        "Programming Language :: Python",
        "Programming Language :: Python :: 2.7",
        "License :: OSI Approved :: MIT License"
    ],
    keywords=[
        "CKAN", "API", "Vector Storer",
    ],
    install_requires=required,
    scripts=['src/bin/pm-run-query'],
)
