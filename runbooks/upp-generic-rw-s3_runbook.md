# UPP - Generic Read/Write S3

Reads CES suggestion messages off kafka and stores them in S3; can also read/write/delete S3 objects via /{uuid} endpoints.

## Code

upp-generic-rw-s3

## Primary URL

<https://upp-prod-delivery-glb.upp.ft.com/__generic-rw-s3/>

## Service Tier

Bronze

## Lifecycle Stage

Production

## Host Platform

AWS

## Architecture

This is an API for reading/writing generic payloads up to S3. It can be setup to read those payloads off Kafka.
Complete API specification can be found [here](https://docs.google.com/document/d/1Ck-o0Le9cXOfm-aVjiGmOT7ZTB5W5fDTsPqGkhzfa-U/edit#heading=h.jwsnnbv7enh5)

## Contains Personal Data

No

## Contains Sensitive Data

No

## Failover Architecture Type

ActiveActive

## Failover Process Type

FullyAutomated

## Failback Process Type

FullyAutomated

## Failover Details

The service is deployed in both Delivery clusters. The failover guide for the cluster is located here:
<https://github.com/Financial-Times/upp-docs/tree/master/failover-guides/delivery-cluster>

## Data Recovery Process Type

NotApplicable

## Data Recovery Details

No need to recovery any data

## Release Process Type

PartiallyAutomated

## Rollback Process Type

Manual

## Release Details

It is deployed with Jenkins job

## Key Management Process Type

Manual

## Key Management Details

To access the job clients need to provide basic auth credentials to log into the k8s clusters.
To rotate credentials you need to login to a particular cluster and update varnish-auth secrets.

## Monitoring

- Delivery PROD EU Health: <https://upp-prod-delivery-eu.upp.ft.com/__health/__pods-health?service-name=generic-rw-s3>
- Delivery PROD US Health : <https://upp-prod-delivery-us.upp.ft.com/__health/__pods-health?service-name=generic-rw-s3>

## First Line Troubleshooting

<https://github.com/Financial-Times/upp-docs/tree/master/guides/ops/first-line-troubleshooting>

## Second Line Troubleshooting

Please refer to the GitHub repository README for troubleshooting information.
