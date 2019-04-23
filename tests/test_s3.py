import io
import re

import botocore.client
import botocore.stub
import pytest

from astromech import s3


@pytest.fixture(scope='module')
def bucket():
    return 'test-bucket'


@pytest.fixture(scope='module')
def key():
    return 'test-key'


def test_client():
    assert s3._client is None
    client = s3.client()
    assert isinstance(client, botocore.client.BaseClient)
    assert s3._client == client
    assert re.fullmatch(r'https?:\/\/s3\.amazonaws\.com', client.meta.endpoint_url)
    s3._client = None


def test_localstack_client(monkeypatch):
    localstack_url = 'http://localhost:4572'
    assert s3._client is None
    with monkeypatch.context() as m:
        m.setenv('LOCALSTACK_S3_URL', localstack_url)
        client = s3.client()
        assert client.meta.endpoint_url == localstack_url
    assert s3.client().meta.endpoint_url == localstack_url
    s3._client = None


def test_parse_uri():
    uri = 's3://bucket/path/to/file'
    assert s3.parse_uri(uri) == ('bucket', 'path/to/file')


def test_parse_non_s3_uri():
    for uri in ('http://aws.amazon.com', 'ftp://ftp.example.com/files/'):
        with pytest.raises(ValueError):
            s3.parse_uri(uri)


def test_to_uri():
    assert s3.to_uri('my_bucket', 'path/to/my/key') == 's3://my_bucket/path/to/my/key'


def test_default_path(monkeypatch):
    # Bucket + key prefix from the env
    with monkeypatch.context() as m:
        m.setenv('S3_BUCKET', 'default-bucket')
        m.setenv('S3_KEY_PREFIX', 'my/test/prefix')
        assert s3.default_path('dir/filename.ext') == ('default-bucket', 'my/test/prefix/dir/filename.ext')
        m.setenv('S3_KEY_PREFIX', '/my/test/prefix/')
        assert s3.default_path('filename.ext') == ('default-bucket', 'my/test/prefix/filename.ext')
    # Only a bucket from the env
    with monkeypatch.context() as m:
        m.setenv('S3_BUCKET', 'default-bucket')
        assert s3.default_path('/dir/filename.ext') == ('default-bucket', 'dir/filename.ext')
        assert s3.default_path('filename.ext', key_prefix='dir') == ('default-bucket', 'dir/filename.ext')
    # Neither bucket nor key prefix
    assert s3.default_path('/filename.ext', 'my-bucket', 'dir/') == ('my-bucket', 'dir/filename.ext')
    assert s3.default_path('/filename.ext', 'my-bucket') == ('my-bucket', 'filename.ext')
    with pytest.raises(ValueError):
        s3.default_path('filename.ext')


def test_exists(bucket, key):
    with botocore.stub.Stubber(s3.client()) as stubber:
        stubber.add_response('head_object', {'ContentLength': 123}, {'Bucket': bucket, 'Key': key})
        assert s3.exists(bucket, key)


def test_does_not_exist(bucket, key):
    expected_params = {'Bucket': bucket, 'Key': key}
    with botocore.stub.Stubber(s3.client()) as stubber:
        stubber.add_client_error(
            'head_object', service_message='Not Found', http_status_code=404, expected_params=expected_params)
        assert not s3.exists(bucket, key)


def test_get_size(bucket, key):
    content_length = 473831
    with botocore.stub.Stubber(s3.client()) as stubber:
        stubber.add_response('head_object', {'ContentLength': content_length}, {'Bucket': bucket, 'Key': key})
        assert s3.get_size(bucket, key) == content_length


def test_get_bytes(bucket, key):
    buf = b'Lorem ipsum dolor sit amet'
    with botocore.stub.Stubber(s3.client()) as stubber:
        service_response = {'Body': io.BytesIO(buf), 'ContentLength': len(buf)}
        stubber.add_response('get_object', service_response, {'Bucket': bucket, 'Key': key})
        assert s3.get_bytes(bucket, key) == buf


def test_get_tags(bucket, key):
    with botocore.stub.Stubber(s3.client()) as stubber:
        tags = {'key1': 'value1', 'key2': '2'}
        service_response = {'TagSet': [{'Key': 'key1', 'Value': 'value1'}, {'Key': 'key2', 'Value': '2'}]}
        stubber.add_response('get_object_tagging', service_response, {'Bucket': bucket, 'Key': key})
        assert s3.get_tags(bucket, key) == tags
        # Empty tag set:
        stubber.add_response('get_object_tagging', {'TagSet': []}, {'Bucket': bucket, 'Key': key})
        assert s3.get_tags(bucket, key) == {}


def test_put_bytes(bucket, key):
    buf = b'Lorem ipsum dolor sit amet'
    service_response = {'ETag': '6bcf86bed8807b8e78f0fc6e0a53079d-380'}
    expected_params = {'Bucket': bucket, 'Key': key, 'Body': buf, 'Tagging': '', 'ACL': 'private'}
    with botocore.stub.Stubber(s3.client()) as stubber:
        stubber.add_response('put_object', service_response, expected_params)
        assert s3.put_bytes(buf, bucket, key) == (bucket, key, len(buf))
        # Now with tags:
        tags = {'key1': 'value1', 'key2': '2'}
        expected_params['Tagging'] = 'key1=value1&key2=2'
        stubber.add_response('put_object', service_response, expected_params)
        assert s3.put_bytes(buf, bucket, key, tags) == (bucket, key, len(buf))
        # Now with a non-default ACL:
        acl = 'public-read'
        expected_params['ACL'] = acl
        stubber.add_response('put_object', service_response, expected_params)
        assert s3.put_bytes(buf, bucket, key, tags, acl) == (bucket, key, len(buf))
