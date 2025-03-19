# README

## Overview

`create-audit` is a tool designed to help you monitor your data lake usage. It collects data from AWS Athena and AWS CloudTrail, and combines it to provide insights into your data lake usage. The tool is based on a Python AWS Lambda function that writes the data to AWS S3. You can query the data using a query engine like AWS Athena, and use it in your own analytic tools and processes.

Collecting audit data is crucial, as it allows you to monitor usage by users and roles. The data collected helps to enhance security controls by:

- Comparing permissions to actual data usage, and revoking unnecessary permissions.
- Identifying security misconfigurations, such as users/roles used by multiple applications.
- Detecting security incidents and data leakage events through anomaly detection.

Monitoring your data lake usage continuously will help you to understand your operation, and control your security and costs.  You can get to a better permissions model by monitoring the actual usage of the data by your users and roles. You can also detect anomalies – which can lead you to find security incidents. 

Installation is easy, and the tool is based on serverless technologies, so you don’t need to maintain any permanent resources. Read on to learn more about the tool and how to install it.

## Installation

The tool is very easy to install. Use the CloudFormation template to create the necessary resources. The template creates the following resources:
- AWS Athena history collection Lambda function (per region)
- AWS Athena events collection Lambda function (in a single main region)

The CloudFormation templates can create the Lambda roles for you, or you can use an existing role. The tool operates across multiple regions, collecting data from all regions into a single table.

## Problem Articulation

To know who did what in Athena, data from the following sources are needed:

- Cloud trail management logs: saves the users and roles
- Athena history: saves the query itself and data scanned data per query

Since the data is saved in two different sources, and the Athena history is accessible only through the API, a Join operation is needed to get the full picture.

## Solution

### Flow

A Python-based Lambda function which inserts daily data into the data lake. Here are the two main steps performed by the function:

1. Read Athena history data through boto3 API and write objects to S3.
2. Join the Athena history and Cloud Trail management logs and write the results to S3.

Once the data is written to S3, you can query and analyze it using Athena. See the examples below.

### Technical Details


#### Cloud Trail Management Logs Table

Cloud trail collects the users/roles and queries done by Athena as part of its management logs. If you don’t have a trail configured, you will have to define one.

`athean-audit` creates an external table for the cloud trail logs and uses it.

#### Athena History Table

History data is available through the AWS Athena API going back 45 days. It contains information which doesn't exist in the cloud trail logs, such as the query itself and the data scanned. `athean-audit` collects the history data to S3, and creates an external table for it.

#### Athena Events Table

The events table holds the joined data, and is used for querying and analyzing the data.

Here is an example SQL query for finding the top users by data scanned in the last 7 days:

```sql
SELECT user, ROUND(SUM(data_scanned) / 1000000000.0, 2) as total_data_scanned_gb
FROM athena_audit.events
WHERE DATE(day) >= DATE_ADD('day', -7, CURRENT_DATE)
GROUP BY user
ORDER BY total_data_scanned_gb DESC
LIMIT 100;
```
