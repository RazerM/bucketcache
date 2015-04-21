import pytest


def pytest_addoption(parser):
    parser.addoption('--run-slow', action='store_true',
                     default=False, help='run slow tests')


def pytest_runtest_setup(item):
    if 'slow' in item.keywords and not item.config.getoption('--run-slow'):
        pytest.skip('Use --run-slow to execute this test.')
