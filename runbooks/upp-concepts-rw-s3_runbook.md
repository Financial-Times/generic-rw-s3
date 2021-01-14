# UPP - Concepts Read/Write S3

Writing JSON representations of concepts to S3 via /concept/{uuid} endpoints.

## Code

upp-concepts-rw-s3

## Primary URL

<https://upp-prod-publish-glb.upp.ft.com/__concepts-rw-s3/>

## Service Tier

Bronze

## Lifecycle Stage

Production

## Delivered By

content

## Supported By

content

## Known About By

- kalin.arsov
- ivan.nikolov
- hristo.georgiev
- elitsa.pavlova
- georgi.ivanov
- dimitar.terziev
- marina.chompalova
- miroslav.gatsanoga
- asparuh.filipov

## Host Platform

AWS

## Architecture

This service provides facade interface in front of concepts normalized S3 store. It handles reading/writing concept payloads up to S3 bucket.
Complete API specification can be found [here](https://docs.google.com/document/d/1Ck-o0Le9cXOfm-aVjiGmOT7ZTB5W5fDTsPqGkhzfa-U/edit#heading=h.jwsnnbv7enh5)

## Contains Personal Data

No

## Contains Sensitive Data

No

## Failover Architecture Type

ActivePassive

## Failover Process Type

FullyAutomated

## Failback Process Type

FullyAutomated

## Failover Details

The service is deployed in both Publish clusters and Delivery EU.
For Publish simply following the failover guide for the cluster, located [here](https://github.com/Financial-Times/upp-docs/tree/master/failover-guides/publishing-cluster), will be enough.
For Delivery a manual scale up of the service needs to be performed for US.

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

- Delivery PROD EU Health: <https://upp-prod-delivery-eu.upp.ft.com/__health/__pods-health?service-name=concepts-rw-s3>
- Delivery PROD US Health : <https://upp-prod-delivery-us.upp.ft.com/__health/__pods-health?service-name=concepts-rw-s3>

## First Line Troubleshooting

<https://github.com/Financial-Times/upp-docs/tree/master/guides/ops/first-line-troubleshooting>

## Second Line Troubleshooting

Please refer to the GitHub repository README for troubleshooting information.
