import os

import boto3
import botocore.stub
import pytest

from astromech import dynamodb


def test_resource():
    assert dynamodb._resource is None
    resource = dynamodb.resource()
    assert isinstance(resource, boto3.resources.base.ServiceResource)
    assert dynamodb._resource == resource


def test_table_no_envvar():
    """Tests that `table()` raises an exception in the absence of the table name env var."""
    assert 'DYNAMODB_TABLE' not in os.environ
    assert dynamodb._table is None
    with pytest.raises(RuntimeError):
        dynamodb.table()


def test_table_using_env(monkeypatch):
    assert dynamodb._table is None
    with monkeypatch.context() as m:
        m.setenv('DYNAMODB_TABLE', 'test-table')
        table = dynamodb.table()
        assert isinstance(table, boto3.dynamodb.table.TableResource)
        assert dynamodb._table == table
    dynamodb._table = None


def test_table_using_arg():
    table = dynamodb.table('my-table')
    assert isinstance(table, boto3.dynamodb.table.TableResource)
    assert dynamodb._table == table
    dynamodb._table = None


def test_exists():
    dynamodb.table('test-table')
    expected_params = {
        'TableName': 'test-table',
        'Key': {'Id': botocore.stub.ANY},
        'ProjectionExpression': 'Id'}
    response = {'Item': {'Id': {'S': '12345'}}, 'ResponseMetadata': {}}
    with botocore.stub.Stubber(dynamodb.resource().meta.client) as stubber:
        stubber.add_response('get_item', response, expected_params)
        assert dynamodb.exists({'Id': '12345'})
    dynamodb._table = None


def test_does_not_exist():
    dynamodb.table('test-table')
    expected_params = {
        'TableName': 'test-table',
        'Key': {'Id': botocore.stub.ANY},
        'ProjectionExpression': 'Id'}
    response = {'ResponseMetadata': {}}
    with botocore.stub.Stubber(dynamodb.resource().meta.client) as stubber:
        stubber.add_response('get_item', response, expected_params)
        assert not dynamodb.exists({'Id': '12345'})
    dynamodb._table = None
