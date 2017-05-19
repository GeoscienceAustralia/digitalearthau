#!/usr/bin/env python3

import versioneer
from setuptools import setup, find_packages

tests_require = ['pytest', 'pytest-cov', 'mock', 'pep8', 'pylint==1.6.4', 'hypothesis', 'compliance-checker']

setup(
    name='datacubenci',
    version=versioneer.get_version(),
    cmdclass=versioneer.get_cmdclass(),

    url='https://github.com/GeoscienceAustralia/ga-datacube-env',
    author='AGDC Collaboration',
    license='Apache License 2.0',

    packages=find_packages(),
    package_data={
        '': ['*.yaml', '*/*.yaml'],
    },
    scripts=[
    ],
    setup_requires=[
        'pytest-runner'
    ],
    install_requires=[
        'click>=5.0',
        'compliance-checker',
        'datacube',
        'python-dateutil',
        'gdal',
        'eodatasets',
        # dev module is used automatically when run interactively.
        'structlog[dev]',
        'DAWG',
        'boltons',
    ],
    tests_require=tests_require,

    entry_points={
        'console_scripts': [
            'datacube-nci-archive = datacubenci.archive:main',
        ]
    },
)
