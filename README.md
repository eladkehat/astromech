# Astromech - AWS Service Utilities for Lambda Functions

Astromech is a collection of utilities that I found myself copying all over my lambda functions and serverless
projects. For the sake of DRYing up my code, I collected them in a single library.

## Service Clients
Astromech implements lazy-initialization for Boto3 service clients:

For each service, there is a global client variable that gets reused between invocations of the labmda function
container. Code that want access to the client achieves this by using a `client()` function that initializes it
if necessary.

Code example:
```python
from astromech import s3, dynamodb

s3.client().get_object(Bucket=..., Key=...)
dynamodb.table().get_item(Key=...)
```

## LocalStack Support Made Easy
The service client initialization functions, look for the environment variable `LOCALSTACK_[SERVICE]_URL`
(for example, `LOCALSTACK_S3_URL`).
If it finds one, then it uses its value as the `endpoint_url` for the Boto3 client function.

All you have to do to run tests with [LocalStack](https://localstack.cloud) is to set these variables in your local
environment. No changes to the code are necessary.
Naturally, this works with any other downloadable version of AWS services, like
[DynamoDB local](https://hub.docker.com/r/amazon/dynamodb-local).


## Utility Functions
There are a few utility functions in each service. These usually save 2-3 lines of boilerplate code. Not a lot,
but for very common functionality that is repeated all across the code base, it becomes worthwhile.

For example, to check if an object exists on S3, you have to send a `head_object` request and catch the exception
if it doesn't exist. With `astromech.s3` you just call `exists(bucket, key)`, much like you would do with a local
file.


## Why "Astromech"?
In the Star Wars universe, astromech is a type of utility droid, the most famous of which (whom?) is R2-D2.

Read more on [Wookiepedia](https://starwars.fandom.com/wiki/Astromech_droid).

![R2-series astromech droids](https://vignette.wikia.nocookie.net/starwars/images/1/1d/Threeartoos.jpg/revision/latest?cb=20060115025836&format=original)
