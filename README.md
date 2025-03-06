# README

## Overview

create-audit is a tool that helps you to monitor your data lake usage. It collects data from Athena and Cloud Trail, and joins it to provide insights about your data lake usage. The tool is based on a Python Lambda function that writes the data to S3. You can query the data using Athena.

The audit trail enabled us to monitor usage by users and roles. The data we collected helps to increase security controls:

- Compared permission to actual data usage, and revoke some permissions.
- Find security misconfigurations like users/roles used by multiple applications.
- Detected security incidents and data leakage events using anomaly detection.

## Installation

Use the cloudformation template to create the resources needed for the tool. The template creates the following resources:
- Athena history collection lambda function (per region)
- Athena events collection lambda function (in a single main region)
The cloud formation templates can create the lambda roles for you, or you can use an existing role.

## Problem Articulation

To know who did what in Athena data from the following sources are needed:

- Cloud trail management logs save the users, roles
- Athena history saves the query itself and data scanned data per query

Since the data is saved in two different sources, and the athena history is accessible only through the API, we need to get and join the data to get the full picture.

## Solution

### Flow

A Python-based Lambda function which inserts daily data into the data lake. Here are the two main steps performed by the function:

1. Read Athena history data through boto3 API and write objects to S3.
2. Join the Athena history and Cloud Trail management logs and write the results to S3.

Once the data is written to S3, you can query and analyze it using Athena. See the examples below.

### Technical Details

#### Athena History Table

The Athena history table is needed for the ETL (Extract Transform Load) process to work. The history data contains data about the query like the data scanned in bytes that we will use. It is possible to keep the data, or delete it after the ETL run. The history is available through the API going back 45 days.

We use a similar function in our Lambda function, which later perform gzip operation on all the workgroups’ files and uploads them to a new partition (folder) in the history table in S3.

#### Cloud Trail Management Logs Table

Cloud trail collects the users/roles and queries done by Athena as part of its management logs. If you don’t have a trail configured you will have to define one.

You have to create an external table for reading your cloud trail logs by Athena.

#### Athena Events Table

The events table will hold the joined data. We will have to create a table for the results. We chose the subset of cloud trail fields which interest us the most – you can use your own set of fields and change the table accordingly.

We have two source tables, one for the scanned data and one for the events – we will join the data, and insert the results to a new table. Both tables should have the daily partition before you can query them.

#### Joining Athena History with Cloud Trail

The last step of the ETL job is used to join and insert the results. We used an `insert into` command which does the following operations:

- Joins the data.
- Converts it to parquet format for better performance and costs.
- Writes the data to the right S3 location.
- Alters the target table.

Once your events table is ready you can query it by Athena. Here is an example SQL for finding the top users by data scanned:

```sql
SELECT user, SUM(data_scanned) as total_data_scanned
FROM events_table
GROUP BY user
ORDER BY total_data_scanned DESC
LIMIT 10;
```
Monitoring your data lake usage continuously will help you to understand your operation, and control your security and costs.  You can get to a better permissions model by monitoring the actual usage of the data by your users and roles. You can also detect anomalies – which can lead you to find security incidents. Athena is one of many services that you have to monitor, and the more services you cover, the better control you have.