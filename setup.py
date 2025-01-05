#!/usr/bin/env python

import os
from setuptools import setup
from setuptools import find_packages

VERSION = open('VERSION').read().strip()

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

with open(os.path.join(os.path.dirname(__file__), "README.rst"), "rt") as f:
    DESCRIPTION = f.read()

BASESPACE_PYTHON_SDK_VERSION = "basespace-python-sdk @ git+https://github.com/emedgene/basespace-python-sdk@0.6.0" \
                               "#subdirectory=src"
BASESPACE_SDK_2_VERSION = "bssh-sdk-2 @ git+https://github.com/emedgene/basespace-python-sdk-v2@1.2.3"

REQUIREMENTS = [BASESPACE_PYTHON_SDK_VERSION, BASESPACE_SDK_2_VERSION, "fs~=2.4", "smart-open~=5.1"]

setup(
    name="fs-basespace",
    author="emedgene",
    author_email="pypi@emedgene.com",
    classifiers=CLASSIFIERS,
    description="Illumina Basespace filesystem for PyFilesystem2",
    install_requires=REQUIREMENTS,
    license="MIT",
    long_description=DESCRIPTION,
    packages=find_packages(),
    keywords=["pyfilesystem", "Illumina", "Basespace"],
    platforms=["any"],
    url="https://github.com/emedgene/fs_basespace",
    download_url="https://github.com/emedgene/fs_basespace/tarball/0.2.0",
    version=VERSION,
    entry_points={"fs.opener": ["basespace = fs_basespace.opener:BASESPACEFSOpener"]},
)
