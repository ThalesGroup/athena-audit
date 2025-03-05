CREATE EXTERNAL TABLE {table} (
  query_id string,
  event_time timestamp,
  user_identity_type string,
  user_identity_principal string,
  user_identity_arn string,
  source_ip string,
  user_agent string,
  workgroup string,
  query string,
  `database` string,
  data_scanned bigint)
PARTITIONED BY (
  region string,
  day string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  's3://{bucket}/{prefix}'