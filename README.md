# Generic Reader/Writer for S3

[![Circle CI](https://circleci.com/gh/Financial-Times/generic-rw-s3.svg?style=shield)](https://circleci.com/gh/Financial-Times/generic-rw-s3)
[![Go Report Card](https://goreportcard.com/badge/github.com/Financial-Times/generic-rw-s3)](https://goreportcard.com/report/github.com/Financial-Times/generic-rw-s3)
[![Coverage Status](https://coveralls.io/repos/github/Financial-Times/generic-rw-s3/badge.svg)](https://coveralls.io/github/Financial-Times/generic-rw-s3)

## system-code: upp-generic-rw-s3 and upp-concepts-rw-s3

## Introduction

An API for reading/writing generic payloads up to S3. It can be setup to read those payloads off Kafka.

## Build and Test

```sh
git clone github.com/Financial-Times/generic-rw-s3  
cd ./generic-rw-s3
go build -mod=readonly
go test -mod=readonly -race ./...
```

## Running locally

```sh
export|set PORT=8080
export|set BUCKET_NAME='bucketName"
export|set AWS_REGION="eu-west-1"
./generic-rw-s3
```

The app assumes that you have correctly set up your AWS credentials by either using the `~/.aws/credentials` file:

```sh
[default]
aws_access_key_id = AKID1234567890
aws_secret_access_key = MY-SECRET-KEY
```

or the default AWS environment variables:

```sh
AWS_ACCESS_KEY_ID=AKID1234567890
AWS_SECRET_ACCESS_KEY=MY-SECRET-KEY
```

There are optional arguments as well:

```sh
export|set BUCKET_PREFIX="bucketPrefix" # adds a prefix folder to all items uploaded
export|set WORKERS=10 # Number of concurrent downloads when downloading all items. Default is 10
```

### Run locally with read from kafka enabled

`./generic-rw-s3 --port=8080 --bucketName="bucketName" --bucketPrefix="bucketPrefix" --awsRegion="eu-west-1" --kafka-address="<kafka_address>" --consumer-group="<consumer_group>" --consumer-topic="<topic_to_read>"`

### Run locally with specified resource path

`./generic-rw-s3 --port=8080 --resourcePath="concepts" --bucketName="bucketName" --bucketPrefix="bucketPrefix" --awsRegion="eu-west-1"`

## Test locally

See Endpoints section.

## Build and deployment

* Docker Hub builds: [coco/generic-rw-s3](https://hub.docker.com/r/coco/generic-rw-s3/)
* Cluster deployment:  [concepts-rw-s3@.service](https://github.com/Financial-Times/pub-service-files), [generic-rw-s3@service](https://github.com/Financial-Times/up-service-files)
* CI provided by CircleCI: [generic-rw-s3](https://circleci.com/gh/Financial-Times/generic-rw-s3)
* Code coverage provided by Coverall: [generic-rw-s3](https://coveralls.io/github/Financial-Times/generic-rw-s3)

## Service Endpoints

For complete API specification see [S3 Read/Write API Endpoint](https://docs.google.com/document/d/1Ck-o0Le9cXOfm-aVjiGmOT7ZTB5W5fDTsPqGkhzfa-U/edit#)

### PUT /UUID

Any payload can be written via the PUT using a unique UUID to identify this payload within the S3 bucket

```sh
curl -H 'Content-Type: application/json' -X PUT -d '{"tags":["tag1","tag2"],"question":"Which band?","answers":[{"id":"a0","answer":"Answer1"},{"id":"a1","answer":"answer2"}]}' http://localhost:8080/123e4567-e89b-12d3-a456-426655440000
```

The `Content-Type` is important as that will be what the file will be stored as.
In addition we will also store transaction ID in S3. It is either provided as request header and if not, it is auto-generated.

When the content is uploaded, the key generated for the item is converted from
`123e4567-e89b-12d3-a456-426655440000` to `<bucket_prefix>/123e4567/e89b/12d3/a456/426655440000`.
The reason we do this is so that it becomes easier to manage/browser for content in the AWS console.
It is also good practice to do this as it means that files get put into different partitions.
This is important if you're writing and pulling content from S3 as it means that content will get written/read from different partitions on S3.

If `bucket_prefix` is present, concepts will always be written in the following format `<bucket_prefix>/123e4567/e89b/12d3/a456/426655440000`.

However if `bucket_prefix` is not present, there is additional `path` parameter functionality for writing/reading concepts to/from a specific partition. A sample request would look like:

```sh
curl -H 'Content-Type: application/json' -X PUT -d '{"tags":["tag1","tag2"],"question":"Which band?","answers":[{"id":"a0","answer":"Answer1"},{"id":"a1","answer":"answer2"}]}' http://localhost:8080/123e4567-e89b-12d3-a456-426655440000?path=TestDirectory
```

when the parameter is present and the content is uploaded, the key generated for the item is converted from
`123e4567-e89b-12d3-a456-426655440000` to `TestDirectory/123e4567/e89b/12d3/a456/426655440000`.

### GET /UUID

This internal read should return what was written to S3

If not found, you'll get a 404 response.

```sh
curl http://localhost:8080/bcac6326-dd23-4b6a-9dfa-c2fbeb9737d9
```

Getting what was written in specific directory

```sh
curl http://localhost:8080/bcac6326-dd23-4b6a-9dfa-c2fbeb9737d9?path=TestDirectory
```

### DELETE /UUID

To delete something from specific directory the `path` parameter should be appended to the request as follows:

```sh
curl .../bcac6326-dd23-4b6a-9dfa-c2fbeb9737d9?path=TestDirectory
```

Will return 204

## Utility endpoints

### GET /

Streams all payloads in a given bucket

To stream the payload a specific directory the `path` parameter should be appended to the request as follows:

```sh
curl .../bcac6326-dd23-4b6a-9dfa-c2fbeb9737d9?path=TestDirectory
```

### GET /__ids

Streams all ids in a given bucket

```sh
curl http://localhost:8080/__ids
```

The return payload will look like:

```sh
{"ID":"dcfa65d6-3849-445e-ac6a-15bc5a17e954"}
{"ID":"2136f8ad-e94e-45cb-b616-336f38533214"}
{"ID":"c9f5337d-0435-477e-b0f5-bd35ff3a4b48"}
{"ID":"7f84a70b-7085-4309-aa8e-304b3759f49f"}
{"ID":"99a0537a-3635-479b-92f7-ba10b63e2f87"}
...
```

If there were concepts also in a directory `TestDirectory` the payload will look like:

```sh
{"ID":"dcfa65d6-3849-445e-ac6a-15bc5a17e954"}
{"ID":"2136f8ad-e94e-45cb-b616-336f38533214"}
{"ID":"c9f5337d-0435-477e-b0f5-bd35ff3a4b48"}
{"ID":"TestDirectory-7f84a70b-7085-4309-aa8e-304b3759f49f"}
{"ID":"TestDirectory-99a0537a-3635-479b-92f7-ba10b63e2f87"}
...
```

### Admin endpoints

Healthchecks: [http://localhost:8080/__health](http://localhost:8080/__health)  
Build Info: [http://localhost:8080/__build-info](http://localhost:8080/__build-info)
GTG: [http://localhost:8080/__gtg](http://localhost:8080/__gtg)

### Other Information

#### Hashing

This service stores a hash of the payload in the metadata of the s3 object on each write. If the ONLY_UPDATES_ENABLED flag is set to true the payload's hash is compared to the stored record. Only records which have been updated or are entirely new will be written. Records that have not been updated will instead return 304 Not Modified. If the ONLY_UPDATES_ENABLED flag is set to false then records will always be updated regardless of the stored hash. The hash can also be bypassed by setting a request header of "X-Ignore-Hash" to true.

#### S3 buckets

For this to work you need to make sure that your AWS credentials has the following policy file on the bucket.

```json
{
    "Version": "2012-10-17",
    "Id": "Policy12345678990",
    "Statement": [
        {
            "Sid": "Stmt12345678990",
            "Effect": "Allow",
            "Principal": {
                "AWS": "arn:aws:iam::<ID>:user/<AWS_KEY_ID>"
            },
            "Action": "s3:*",
            "Resource": [
                "arn:aws:s3:::<BUCKET_NAME>",
                "arn:aws:s3:::<BUCKET_NAME>/*"
            ]
        }
    ]
}
```
