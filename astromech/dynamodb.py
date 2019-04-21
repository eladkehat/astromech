import os
from typing import Optional

import boto3
import boto3.dynamodb.table

_resource = None
"""A DynamoDb service resource, initialized lazily by `resource()` or `table()`.

The resource object is global, so that it gets reused between invocations by the lambda
function container.

Do not use this directly! Instead, use the `resource()` function to get an initialized
service resource.
"""

_table = None
"""A DynamoDB Table, initialized lazily by `table()`.

The table object is global, so that it gets reused between invocations by the lambda
function container.

Do not use this directly! Instead, use the `table()` function to get an initialized table.
"""


def resource() -> boto3.resources.base.ServiceResource:
    """Returns a DynamoDB service resource.

    This function always returns the global resource object, initializing it if necessary.
    Use it inside your code whenever you need a DynamoDB service resource, rather than creating
    a new one or using the global resource object directly.

    Returns:
        The DynamoDB service resource object.
    """
    global _resource
    if _resource is None:
        _resource = boto3.resource('dynamodb')
    return _resource


def table(table_name: Optional[str] = None) -> boto3.dynamodb.table.TableResource:
    """Returns a DynamoDB table object.

    The table takes its name from the `table_name` argument, or from the environment variable
    "DYNAMODB_TABLE". If both are missing the function raises an error.
    Once the global `_table` variable is initialized, you can call this function without supplying
    a table name.

    This function always returns the global table object, initializing it if necessary.
    Use it inside your code whenever you need an DynamoDB table, rather than creating a new
    one or using the global table object directly.

    Args:
        table_name: Name of the DynamoDB table.
            If missing, defaults to the value from the environment variable "DYNAMODB_TABLE".

    Returns:
        The DynamoDB table object.
    """
    global _table
    if _table is None:
        if not table_name:
            if 'DYNAMODB_TABLE' not in os.environ:
                message = 'astromech.dynamodb requires that the environment variable "DYNAMODB_TABLE" be set!'
                raise(RuntimeError(message))
            table_name = os.environ['DYNAMODB_TABLE']
        _table = resource().Table(table_name)
    return _table


def exists(key: dict) -> bool:
    """Checks whether an item exists in the DynamoDB table.

    Args:
        key: The primary key of the item to check.

    Returns:
        True if an item with the specified key exists, False otherwise.
    """
    response = table().get_item(Key=key, ProjectionExpression=','.join(key.keys()))
    return 'Item' in response
