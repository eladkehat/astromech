import json
import os
from typing import Generator

import boto3
import botocore

_client = None
"""A boto SQS client, initialized lazily by `client()`.

The client object is global, so that it gets reused between invocations by the lambda
function container.

Do not use this directly! Instead, use the `client()` function to get an initialized client.
"""


def client() -> botocore.client.BaseClient:
    """Returns an SQS client object.

    This function always returns the global client object, initializing it if necessary.
    Use it inside your code whenever you need an SQS client, rather than creating a new one
    or using the global client object directly.

    If the environment variable "LOCALSTACK_SQS_URL" is present, uses that as the endpoint_url,
    instead of the real AWS URL.

    Returns:
        The SQS client object.
    """
    global _client
    if _client is None:
        endpoint_url = os.environ.get('LOCALSTACK_SQS_URL')
        # If endpoint_url is None, botocore constructs the default AWS URL
        _client = boto3.client('sqs', endpoint_url=endpoint_url)
    return _client


def parse_event(event: dict) -> Generator:
    """Yields messages from a SQS events.

    Use this in lambda functions that receive events from SQS, where the SQS queue is subscribed
    to an SNS topic with raw message delivery.

    Attempts to deserialize each record body from JSON. If the body isn't JSON, returns it as-is.

    Args:
        event: The event from `lambda_handler()`.

    Yields:
        The deserialized message bodies from the event records.
    """
    for record in event['Records']:
        try:
            item = json.loads(record['body'])
        except ValueError:
            item = record['body']
        yield item
