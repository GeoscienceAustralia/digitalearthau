#!/usr/bin/env python3

from setuptools import setup, find_packages

import versioneer

tests_require = ['pytest', 'pytest-cov', 'mock', 'pep8', 'pylint==1.6.4', 'hypothesis', 'compliance-checker']

setup(
    name='digitalearthau',
    version=versioneer.get_version(),
    cmdclass=versioneer.get_cmdclass(),

    url='https://github.com/GeoscienceAustralia/digitalearthau',
    author='Geoscience Australia',
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
            'dea-archive = digitalearthau.archive:main',
            'dea-clean = digitalearthau.cleanup:main',
            'dea-coherence = digitalearthau.coherence:main',
            'dea-duplicates = digitalearthau.duplicates:main',
            'dea-move = digitalearthau.move:main',
            'dea-sync = digitalearthau.sync:cli',
        ]
    },
)
