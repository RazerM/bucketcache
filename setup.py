import re
import sys

from setuptools import setup, find_packages
from setuptools.command.test import test as TestCommand


INIT_FILE = 'bucketcache/__init__.py'
init_data = open(INIT_FILE).read()

metadata = dict(re.findall("__([a-z]+)__ = '([^']+)'", init_data))

AUTHOR_EMAIL = metadata['author']
VERSION = metadata['version']
LICENSE = metadata['license']
DESCRIPTION = metadata['description']

AUTHOR, EMAIL = re.match(r'(.*) <(.*)>', AUTHOR_EMAIL).groups()

requires = {
    'boltons',
    'decorator>=4.0.2',
    'logbook>=0.12.5',
    'python-dateutil',
    'represent>=1.4.0',
    'six>=1.9.0',
    'setuptools>=17.1',
}


def add_to_extras(extras_require, dest, source):
    """Add dependencies from `source` extra to `dest` extra, handling
    conditional dependencies.
    """
    for key, deps in list(extras_require.items()):
        extra, _, condition = key.partition(':')
        if extra == source:
            if condition:
                try:
                    extras_require[dest + ':' + condition] |= deps
                except KeyError:
                    extras_require[dest + ':' + condition] = deps
            else:
                try:
                    extras_require[dest] |= deps
                except KeyError:
                    extras_require[dest] = deps

extras_require = dict()

extras_require[':python_version<"3.4"'] = {'pathlib'}

extras_require['test'] = {
    'msgpack-python',
    'pytest',
    'pytest-benchmark',
    'pytest-cov',
    'pytest-xdist',
}

extras_require['test:python_version<"3.3"'] = {'mock'}

extras_require['dev'] = {
    'clint',
    'packaging',
    'shovel',
    'sphinx',
    'twine',
}

add_to_extras(extras_require, 'dev', 'test')


class PyTest(TestCommand):
    user_options = [('pytest-args=', 'a', "Arguments to pass to py.test")]

    def initialize_options(self):
        TestCommand.initialize_options(self)
        self.pytest_args = []

    def finalize_options(self):
        TestCommand.finalize_options(self)
        self.test_args = []
        self.test_suite = True

    def run_tests(self):
        # import here, cause outside the eggs aren't loaded
        import pytest
        errno = pytest.main(self.pytest_args)
        sys.exit(errno)

setup(
    name='BucketCache',
    version=VERSION,
    description=DESCRIPTION,
    long_description=open('README').read(),
    author=AUTHOR,
    author_email=EMAIL,
    url='https://github.com/RazerM/bucketcache',
    packages=find_packages(),
    cmdclass={'test': PyTest},
    classifiers=[
        'Development Status :: 4 - Beta',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
    ],
    license=LICENSE,
    install_requires=requires,
    extras_require=extras_require,
    setup_requires=['setuptools>=17.1'])
