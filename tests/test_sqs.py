import json
import pathlib
import re

import botocore.client

from astromech import sqs


def test_client():
    assert sqs._client is None
    client = sqs.client()
    assert isinstance(client, botocore.client.BaseClient)
    assert re.fullmatch(r'https?:\/\/queue\.amazonaws\.com', client.meta.endpoint_url)
    assert sqs._client == client
    sqs._client = None


def test_localstack_client(monkeypatch):
    localstack_url = 'http://localhost:4576'
    assert sqs._client is None
    with monkeypatch.context() as m:
        m.setenv('LOCALSTACK_SQS_URL', localstack_url)
        client = sqs.client()
        assert client.meta.endpoint_url == localstack_url
    assert sqs.client().meta.endpoint_url == localstack_url
    sqs._client = None


def test_parse_event():
    path = pathlib.Path(__file__).parent / 'data' / 'sqs_event.json'
    event = json.loads(path.read_text())
    items = [json.loads(event['Records'][0]['body']), event['Records'][1]['body']]
    assert all(i == j for i, j in zip(sqs.parse_event(event), items))
