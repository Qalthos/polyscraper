from setuptools import setup
import multiprocessing
import logging

setup(
    name='polyscraper',
    version='0.1',
    install_requires=[
        "SQLAlchemy",
        "BeautifulSoup4",
        "python-magic",
        "twill",
        "mechanize",
        "knowledge",
    ],
    tests_require=[
        'nose',
    ],
    test_suite='nose.collector',
)
