#!/usr/bin/env python3

from setuptools import setup, find_packages

tests_require = ['pytest', 'pytest-cov', 'mock', 'pycodestyle', 'pylint',
                 'hypothesis', 'compliance-checker', 'yamllint']

extras_require = {
    'doc': ['Sphinx', 'nbsphinx', 'setuptools', 'sphinx_rtd_theme', 'IPython', 'jupyter_sphinx',
            'recommonmark'],
    'scan': ['DAWG'],
    'test': tests_require,
}

setup(
    name='digitalearthau',

    use_scm_version=True,
    url='https://github.com/GeoscienceAustralia/digitalearthau',
    author='Geoscience Australia',
    author_email='damien.ayers@ga.gov.au',
    license='Apache License 2.0',

    packages=find_packages(
        exclude=('tests', 'tests.*',
                 'integration_tests', 'integration_tests.*')
    ),
    package_data={
        '': ['*.yaml', '*/*.yaml'],
    },
    include_package_data=True,
    setup_requires=[
        'pytest-runner',
        'setuptools_scm',
    ],
    install_requires=[
        'attrs>=19.2.0',
        'cattrs',
        'colorama',  # Needed for structlog's CLI output.
        'click>=5.0',
        'datacube[celery] >= 1.8',
        'python-dateutil',
        'gdal',
        'eodatasets3>=0.28.0',
        'structlog',
        'boltons',
        'lxml',
        'pydash',
    ],
    tests_require=tests_require,
    extras_require=extras_require,

    entry_points={
        'console_scripts': [
            'dea-clean = digitalearthau.cleanup:cli',
            'dea-coherence = digitalearthau.coherence:main',
            'dea-duplicates = digitalearthau.duplicates:cli',
            'dea-harvest = digitalearthau.harvest.iso19115:main',
            'dea-move = digitalearthau.move:cli',
            'dea-submit-ingest = digitalearthau.submit.ingest:cli',
            'dea-submit-ncmler = digitalearthau.submit.ncmler:cli',
            'dea-submit-sync = digitalearthau.sync.submit_job:main',
            'dea-sync = digitalearthau.sync:cli',
            'dea-stacker = digitalearthau.stacker:cli',
            'dea-system = digitalearthau.system:cli',
            'dea-test-env = digitalearthau.test_env:cli',
        ]
    },
)
