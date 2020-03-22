#!/usr/bin/env python

from setuptools import setup, find_packages


setup(
    name='indigo-kafka-receiver',
    author='daveb@smurfless.com',
    url='',
    versioning='dev',
    setup_requires=['setupmeta'],
    dependency_links=['https://pypi.org/project/setupmeta'],
    include_package_data=True,
    python_requires='>=3.7',
    install_requires=[
        'kafka-python',
    ],
    extras_require={
        'dev': [
            'behave',
            'flake8',
            'invoke',
            'tox',
            'mypy',
            'pytest'
        ]
    },
    entry_points='''
        [console_scripts]
        indigo-kafka-receiver=main.py
    ''',
)
