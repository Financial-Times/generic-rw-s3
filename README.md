# Generic Reader/Writer for S3 (generic-rw-s3)
[![Circle CI](https://circleci.com/gh/Financial-Times/generic-rw-s3.svg?style=shield)](https://circleci.com/gh/Financial-Times/generic-rw-s3)[![Go Report Card](https://goreportcard.com/badge/github.com/Financial-Times/generic-rw-s3)](https://goreportcard.com/report/github.com/Financial-Times/generic-rw-s3) [![Coverage Status](https://coveralls.io/repos/github/Financial-Times/generic-rw-s3/badge.svg)](https://coveralls.io/github/Financial-Times/generic-rw-s3)
 
__An API for reading/writing generic payloads up to S3. It can be setup to read those payloads off Kafka

## Installation

For the first time:

`go get github.com/Financial-Times/generic-rw-s3`

or update:

`go get -u github.com/Financial-Times/generic-rw-s3`


## Running

TODOcd gen  

## Endpoints

TODO: Add examples

### PUT /UUID

Any payload can be written via the PUT using a unique UUID to identify this payload within the S3 bucket

### GET /UUID
This internal read should return what got written to S3

If not found, you'll get a 404 response.

### GET /
Streams all payloads in a given bucket

### GET /__ids
Streams all ids in a given bucket

### DELETE /UUID
Will return 204 if successful, 404 if not found

### Admin endpoints

Healthchecks: [http://localhost:8080/__health](http://localhost:8080/__health)
Ping: [http://localhost:8080/ping](http://localhost:8080/ping) or [http://localhost:8080/__ping](http://localhost:8080/__ping)
Build Info: [http://localhost:8080/build-info](http://localhost:8080/build-info) or [http://localhost:8080/build-info](http://localhost:8080/__build-info) 
GTG: [http://localhost:8080/build-info](http://localhost:8080/__gtg) 
