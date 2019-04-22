import json
import os
from typing import Any, Union

import boto3
import botocore

from astromech.logging import logger

_client = None
"""A boto SNS client, initialized lazily by `client()`.

The client object is global, so that it gets reused between invocations by the lambda
function container.

Do not use this directly! Instead, use the `client()` function to get an initialized client.
"""


def client() -> botocore.client.BaseClient:
    """Returns an SNS client object.

    This function always returns the global client object, initializing it if necessary.
    Use it inside your code whenever you need an SNS client, rather than creating a new one
    or using the global client object directly.

    If the environment variable "LOCALSTACK_SNS_URL" is present, uses that as the endpoint_url,
    instead of the real AWS URL.

    Returns:
        The SNS client object.
    """
    global _client
    if _client is None:
        endpoint_url = os.environ.get('LOCALSTACK_SNS_URL')
        # If endpoint_url is None, botocore constructs the default AWS URL
        _client = boto3.client('sns', endpoint_url=endpoint_url)
    return _client


def publish(
    topic_arn: str, context: Any, payload: dict, extra_attributes: dict = {}, subject: Union[str, None] = None
) -> int:
    """Publishes a message to the specified topic on SNS.

    The function name and version from the context are added automatically as message attributes.
    You can provide additional attributes using the extra_attributes parameter.

    Args:
        context: The context object from `lambda_handler()`.
        payload: The event payload. Must be JSON-serializeable.
        extra_attributes: Event attributes in addition to / overriding the defaults.
            Messages are published with 'sender' and 'sender_version' attributes that contain
            the function name and version from the context respectively.
        subject: An optional subject for the event. If you do not provide one, a default subject
            is generated using the function name from the context.

    Returns:
        The unique message id on SNS.
    """
    attributes = {
        'sender': {'DataType': 'String', 'StringValue': context.function_name},
        'sender_version': {'DataType': 'String', 'StringValue': context.function_version}}
    attributes.update(extra_attributes)
    subject = subject or f'Message from {context.function_name}'
    logger.debug(f'Publishing message: {payload} to topic: {topic_arn}')
    response = client().publish(
        TopicArn=topic_arn,
        Subject=subject,
        Message=json.dumps(payload),
        MessageAttributes=attributes)
    message_id = response["MessageId"]
    logger.debug(f'Message published. Message id: {message_id}')
    return message_id


def publish_to_bus(
    context: Any, payload: dict, extra_attributes: dict = {}, subject: Union[str, None] = None
) -> int:
    """A convenience function that publishes to the application message bus topic on SNS.

    Calls `publish()` with a default topic ARN.
    The message bus ARN must be specified by the environment variable "MESSAGE_BUS_ARN".
    """
    message_bus_arn = os.environ['MESSAGE_BUS_ARN']
    return publish(message_bus_arn, context, payload, extra_attributes, subject)
