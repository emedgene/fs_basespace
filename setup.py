#!/usr/bin/env python

import os
from setuptools import setup
from setuptools import find_packages

__version__ = "defined_below"
with open(os.path.join(os.path.dirname(__file__), "fs_basespace/_version.py")) as f:
    exec(f.read())

CLASSIFIERS = [
    "Development Status :: 4 - Beta",
    "Intended Audience :: Developers",
    "Operating System :: OS Independent",
    "Programming Language :: Python",
    "Programming Language :: Python :: 2.7",
    "Programming Language :: Python :: 3.3",
    "Programming Language :: Python :: 3.4",
    "Programming Language :: Python :: 3.5",
    "Programming Language :: Python :: 3.6",
    "Programming Language :: Python :: 3.7",
    "Topic :: System :: Filesystems",
]

with open(os.path.join(os.path.dirname(__file__), "README.md"), "rt") as f:
    DESCRIPTION = f.read()

REQUIREMENTS = ["basespace-python-sdk", "fs", "six", "smart_open"]

setup(
    name="fs-basespace",
    author="vindex10",
    classifiers=CLASSIFIERS,
    description="Illumina Basespace filesystem for PyFilesystem2",
    install_requires=REQUIREMENTS,
    license="MIT",
    long_description=DESCRIPTION,
    packages=find_packages(),
    keywords=["pyfilesystem", "Illumina", "Basespace"],
    platforms=["any"],
    url="https://github.com/emedgene/fs_basespace",
    version=__version__,
    entry_points={"fs.opener": ["basespace = fs_basespace.opener:BASESPACEFSOpener"]},
)
