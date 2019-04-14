import json
import pathlib

import botocore.client

from astromech import sqs


def test_client():
    assert sqs._client is None
    client = sqs.client()
    assert isinstance(client, botocore.client.BaseClient)
    assert sqs._client == client


def test_parse_event():
    path = pathlib.Path(__file__).parent / 'data' / 'sqs_event.json'
    event = json.loads(path.read_text())
    items = [json.loads(event['Records'][0]['body']), event['Records'][1]['body']]
    assert all(i == j for i, j in zip(sqs.parse_event(event), items))
