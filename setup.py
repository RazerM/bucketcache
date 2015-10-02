import re
import sys

from setuptools import setup, Command, find_packages


INIT_FILE = 'bucketcache/__init__.py'
init_data = open(INIT_FILE).read()

metadata = dict(re.findall("__([a-z]+)__ = '([^']+)'", init_data))

AUTHOR_EMAIL = metadata['author']
VERSION = metadata['version']
LICENSE = metadata['license']
DESCRIPTION = metadata['description']

AUTHOR, EMAIL = re.match(r'(.*) <(.*)>', AUTHOR_EMAIL).groups()

requires = ['boltons', 'decorator>=4.0.2', 'logbook', 'python-dateutil',
            'represent>=1.3.0', 'six>=1.9.0']
if sys.version_info[:2] < (3, 4):
    requires.append('pathlib')

extras_require = {}
extras_require.update(
    test=[
        'msgpack-python',
        'pytest',
        'pytest-benchmark',
        'pytest-cov',
        'pytest-xdist',
    ])

if sys.version_info < (3, 3):
    extras_require['test'].append('mock')

extras_require.update(
    dev=[
        'clint',
        'packaging',
        'shovel',
        'sphinx',
        'twine',
    ] + extras_require['test'])


class PyTest(Command):
    """Allow 'python setup.py test' to run without first installing pytest"""
    user_options = []

    def initialize_options(self):
        pass

    def finalize_options(self):
        pass

    def run(self):
        import subprocess
        import sys
        errno = subprocess.call([sys.executable, 'runtests.py'])
        raise SystemExit(errno)

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
    extras_require=extras_require)
