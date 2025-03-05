CREATE EXTERNAL TABLE {table} (
    userIdentity STRUCT<
        type:STRING,
        principalid:STRING,
        arn:STRING,
        accountid:STRING,
        invokedby:STRING,
        accesskeyid:STRING,
        userName:STRING>,
    eventTime STRING,
    eventSource STRING,
    eventName STRING,
    sourceIpAddress STRING,
    userAgent STRING,
    requestParameters STRING,
    responseElements STRING
)
PARTITIONED BY (region string, year string, month string, day string)
ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
STORED AS INPUTFORMAT 'com.amazon.emr.cloudtrail.CloudTrailInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://{bucket}/{prefix}'