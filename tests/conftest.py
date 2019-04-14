import collections

import pytest


@pytest.fixture(scope='module')
def context():
    """A mock for the lambda_handler context parameter."""
    Context = collections.namedtuple('Context', 'function_name function_version')
    return Context('TestFunction', '1')
