import os

import boto3
import botocore

_client = None
"""A boto SSM (AWS Systems Manager) client, initialized lazily by `client()`.

The client object is global, so that it gets reused between invocations by the lambda
function container.

Do not use this directly! Instead, use the `client()` function to get an initialized client.
"""


def client() -> botocore.client.BaseClient:
    """Returns an SSM client object.

    This function always returns the global client object, initializing it if necessary.
    Use it inside your code whenever you need an SSM client, rather than creating a new one
    or using the global client object directly.

    If the environment variable "LOCALSTACK_SSM_URL" is present, uses that as the endpoint_url,
    instead of the real AWS URL.

    Returns:
        The SSM client object.
    """
    global _client
    if _client is None:
        endpoint_url = os.environ.get('LOCALSTACK_SSM_URL')
        # If endpoint_url is None, botocore constructs the default AWS URL
        _client = boto3.client('ssm', endpoint_url=endpoint_url)
    return _client


def get_param_value(param: str, decrypt: bool) -> str:
    """Returns the value of the specified parameter from SSM ParameterStore.

    Args:
        param: The name of the parameter in ParameterStore.
        decrypt: Whether to decrypt the parameter.

    Returns:
        The parameter value.

    Raises:
        Raises Any exceptions raised by boto due to missing parameters etc.
    """
    response = client().get_parameter(Name=param, WithDecryption=decrypt)
    return response['Parameter']['Value']
