import json
import re

import botocore.client
import botocore.stub
import pytest

from astromech import sns


def test_client():
    assert sns._client is None
    client = sns.client()
    assert isinstance(client, botocore.client.BaseClient)
    assert sns._client == client
    assert re.fullmatch(r'https?:\/\/sns\.[a-z\-0-9]+\.amazonaws\.com', client.meta.endpoint_url)
    sns._client = None


def test_localstack_client(monkeypatch):
    localstack_url = 'http://localhost:4575'
    assert sns._client is None
    with monkeypatch.context() as m:
        m.setenv('LOCALSTACK_SNS_URL', localstack_url)
        client = sns.client()
        assert client.meta.endpoint_url == localstack_url
    assert sns.client().meta.endpoint_url == localstack_url
    sns._client = None


def params_for_publish(context, subject):
    topic_arn = 'arn:aws:sns:us-east-1:1234567890:test-topic'
    payload = {
        'arg1': 17,
        'arg2': 'value 2'}
    extra_attributes = {
        'att1': {'DataType': 'String', 'StringValue': 'Attribute 1'},
        'att2': {'DataType': 'Number', 'StringValue': '157'}}
    attributes = {
        'sender': {'DataType': 'String', 'StringValue': context.function_name},
        'sender_version': {'DataType': 'String', 'StringValue': context.function_version}}
    attributes.update(extra_attributes)
    message_id = '2013907e-de73-5f67-bccd-c57c0271179c'
    expected_params = {
        'TopicArn': topic_arn,
        'Subject': subject,
        'Message': json.dumps(payload),
        'MessageAttributes': attributes}
    if subject is None:
        expected_params['Subject'] = f'Message from {context.function_name}'
    return (topic_arn, payload, extra_attributes, message_id, expected_params)


@pytest.mark.parametrize('subject', ['My Subject', None])
def test_publish(context, subject):
    topic_arn, payload, extra_attributes, message_id, expected_params = params_for_publish(context, subject)
    with botocore.stub.Stubber(sns.client()) as stubber:
        stubber.add_response('publish', {'MessageId': message_id}, expected_params)
        assert sns.publish(topic_arn, context, payload, extra_attributes, subject) == message_id


@pytest.mark.parametrize('subject', ['My Subject', None])
def test_publish_to_bus(context, subject, monkeypatch):
    topic_arn, payload, extra_attributes, message_id, expected_params = params_for_publish(context, subject)
    monkeypatch.setenv('MESSAGE_BUS_ARN', topic_arn)
    with botocore.stub.Stubber(sns.client()) as stubber:
        stubber.add_response('publish', {'MessageId': message_id}, expected_params)
        assert sns.publish_to_bus(context, payload, extra_attributes, subject) == message_id
