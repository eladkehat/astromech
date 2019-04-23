import os
from typing import Tuple, Union
import urllib.parse

import boto3
import botocore

from astromech.logging import logger

_client = None
"""A boto S3 client, initialized lazily by `client()`.

The client object is global, so that it gets reused between invocations by the lambda
function container.

Do not use this directly! Instead, use the `client()` function to get an initialized client.
"""


def client() -> botocore.client.BaseClient:
    """Returns an S3 client object.

    This function always returns the global client object, initializing it if necessary.
    Use it inside your code whenever you need an S3 client, rather than creating a new one
    or using the global client object directly.

    If the environment variable "LOCALSTACK_S3_URL" is present, uses that as the endpoint_url,
    instead of the real AWS URL.

    Returns:
        The S3 client object.
    """
    global _client
    if _client is None:
        endpoint_url = os.environ.get('LOCALSTACK_S3_URL')
        # If endpoint_url is None, botocore constructs the default AWS URL
        _client = boto3.client('s3', endpoint_url=endpoint_url)
    return _client


def parse_uri(uri: str) -> Tuple[str, str]:
    """Parse a S3 URI.

    Args:
        uri: A S3 URI in the format s3://bucket/key.

    Returns:
        A tuple with the bucket and key.
    """
    scheme, bucket, key, _, _, _ = urllib.parse.urlparse(uri)
    if not scheme == 's3':
        raise ValueError(f'Not a S3 URI: {uri}')
    return (bucket, key.lstrip('/'))


def to_uri(bucket: str, key: str) -> str:
    """Construct a S3 URI.

    Args:
        bucket: The S3 bucket name.
        key: The S3 key.

    Returns:
        A S3 URI in the format s3://bucket/key.
    """
    return f's3://{bucket}/{key}'


def default_path(
    filename: str, bucket: Union[str, None] = None, key_prefix: Union[str, None] = None
) -> Tuple[str, str]:
    """Returns a default S3 path for a given filename using env variables.

    For lambda functions that interact with a specific bucket and / or key prefix,
    use environment variables to set those bucket and key prefix and then use this
    function to use them automatically.

    The bucket env variable key is "S3_BUCKET".
    The key prefix env variable key is "S3_KEY_PREFIX".

    Args:
        filename: If there is no key prefix, filename is used as the key.
            If there is a key prefix, filename is used as the suffix, joined to
            the key prefix with a forward slash ("/").
        bucket: Override the default S3 bucket name read from the env var.
            A bucket *must* be supplied - either explicitly or via an env var.
        key_prefix: Override the default S3 key prefix from the env var.
            A key prefix is optional.

    Returns:
        A 2-tuple:
        - The resolved bucket.
        - The resolved key.

    Raises:
        ValueEror if a bucket was not specified and could not be read from the
        env var.
    """
    bucket = bucket or os.environ.get('S3_BUCKET')
    if not bucket:
        raise(ValueError('Missing value for the S3 bucket name.'))
    key_prefix = key_prefix or os.environ.get('S3_KEY_PREFIX')
    if key_prefix:
        key = f'{key_prefix.strip("/")}/{filename.lstrip("/")}'
    else:
        key = filename.lstrip('/')
    return (bucket, key)


def exists(bucket: str, key: str) -> bool:
    """Checks whether an object exists on S3.

    Note that this function only works for objects ("files") in S3, not key prefixes.
    For example, if there's an object with key "path/to/object.txt", calling this function
    with "path/to/" returns False.

    Args:
        bucket: The S3 bucket name.
        key: The S3 key.

    Returns:
        True if an object exists at the specified bucket and key, False otherwise.
        This function may also return False if the client lacks permissions.
    """
    try:
        client().head_object(Bucket=bucket, Key=key)
    except botocore.client.ClientError:
        return False
    else:
        return True


def get_size(bucket: str, key: str) -> int:
    """Gets the size of an object on S3.

    Args:
        bucket: The S3 bucket name.
        key: The S3 key.

    Returns:
        The size of the object, in bytes.
    """
    response = client().head_object(Bucket=bucket, Key=key)
    return response['ContentLength']


def get_bytes(bucket: str, key: str) -> bytes:
    """Gets the contents of an object on S3.

    Args:
        bucket: The source S3 bucket name.
        key: The source S3 key.

    Returns:
        The object, as a bytes buffer.
    """
    logger.debug(f'Reading from s3://{bucket}/{key}')
    response = client().get_object(Bucket=bucket, Key=key)
    return response['Body'].read()


def get_tags(bucket: str, key: str) -> dict:
    """Gets an S3 object's tags as a proper dict.

    Converts the boto3 S3 client `get_object_tagging()` TagSet response like:
    'TagSet': [
        {
            'Key': 'my key',
            'Value': 'my value'
        },
    ]

    to the Pythonic:
    {'my key': 'my value'}

    Args:
        bucket: The S3 bucket name.
        key: The S3 key.

    Returns:
        The object tags, as a dict.
    """
    logger.debug(f'Reading tags from s3://{bucket}/{key}')
    response = client().get_object_tagging(Bucket=bucket, Key=key)
    return dict((tag['Key'], tag['Value']) for tag in response['TagSet'])


def put_bytes(buf: bytes, bucket: str, key: str, tags: dict = {}, acl: str = 'private') -> Tuple[str, str, int]:
    """Writes a buffer to S3.

    Args:
        buf: A bytes buffer.
        bucket: The target S3 bucket name.
        key: The target S3 key.
        tags: Tags to assign to the object.
            Note that allowed characters for tags are Unicode letters, whitespace, and numbers, plus the following
            special characters: + - = . _ : /
            For additional characters, use base-64 encoding.
        acl: The canned ACL for the object.
            For options see: https://docs.aws.amazon.com/AmazonS3/latest/dev/acl-overview.html#canned-acl

    Returns:
        A 3-tuple:
        - The bucket.
        - The key.
        - The number of bytes written (length of the buffer).
    """
    logger.debug(f'Writing {len(buf)} bytes to s3://{bucket}/{key}')
    tagging = urllib.parse.urlencode(tags)
    client().put_object(Bucket=bucket, Key=key, Body=buf, Tagging=tagging, ACL=acl)
    return (bucket, key, len(buf))
