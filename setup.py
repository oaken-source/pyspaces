
from os.path import join, dirname
from setuptools import setup


setup(
    name='pyspaces',
    version='0.0.1',

    description='a python library for distributed tuple spaces',
    long_description=open(join(dirname(__file__), 'README.md')).read(),

    packages=[
        'pyspaces',
    ],

    install_requires=[],

    test_suite='tests',
    tests_require=[
        'pytest',
    ],

    setup_requires=['pytest_runner'],
)

