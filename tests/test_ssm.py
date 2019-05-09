import re

import botocore.client

from astromech import ssm


def test_client():
    assert ssm._client is None
    client = ssm.client()
    assert isinstance(client, botocore.client.BaseClient)
    assert re.fullmatch(r'https?:\/\/ssm\.[a-z\-0-9]+\.amazonaws\.com', client.meta.endpoint_url)
    assert ssm._client == client
    ssm._client = None


def test_localstack_client(monkeypatch):
    localstack_url = 'http://localhost:4583'
    assert ssm._client is None
    with monkeypatch.context() as m:
        m.setenv('LOCALSTACK_SSM_URL', localstack_url)
        client = ssm.client()
        assert client.meta.endpoint_url == localstack_url
    assert ssm.client().meta.endpoint_url == localstack_url
    ssm._client = None


def test_get_param_value():
    param_name = '/My/Param'
    response = {  # This is a simplified version of the response, with some fields removed
        'Parameter': {
            'Name': param_name,
            'Type': 'SecureString',
            'Value': 's3cret',
            'Version': 1},
        'ResponseMetadata': {
            'HTTPStatusCode': 200,
            'HTTPHeaders': {
                'x-amzn-requestid': '2cdb1744-0948-4804-a7ab-f3df27dcd213',
                'content-type': 'application/x-amz-json-1.1',
                'content-length': '222',
                'date': 'Mon, 06 May 2019 06:57:03 GMT'},
            'RetryAttempts': 0}}
    with botocore.stub.Stubber(ssm.client()) as stubber:
        stubber.add_response('get_parameter', response, {'Name': param_name, 'WithDecryption': True})
        assert ssm.get_param_value(param_name, True) == 's3cret'
