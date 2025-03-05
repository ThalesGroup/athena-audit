import logging
import os
import time
from enum import Enum
from pathlib import Path
from typing import Generator, List, Dict

import boto3

from common_utils import (
    validate_day_range,
    get_day_back,
    get_yesterday,
    get_days,
    clear_folder,
)

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def get_db_name():
    return os.environ.get("DB_NAME", "default")


def get_workgroup():
    return os.environ.get("WORKGROUP", "primary")


def get_query_timeout() -> int:
    return int(os.environ.get("DEFAULT_QUERY_TIMEOUT", "300"))


def get_athena_output_bucket():
    return os.environ.get("ATHENA_OUTPUT_BUCKET", os.environ["BUCKET"])


def get_athena_output_folder():
    return os.environ.get("ATHENA_OUTPUT_FOLDER", "athena_audit/temp")


def get_regions() -> List[str]:
    return os.environ.get("REGIONS", os.environ["AWS_REGION"]).split(",")


class TableType(Enum):
    CLOUD_TRAIL = (0,)
    HISTORY = (1,)
    EVENTS = (2,)

    @property
    def table_name(self):
        return (
            f"{get_db_name()}.{os.environ.get(self.name + '_TABLE', self.name.lower())}"
        )

    @property
    def bucket(self):
        return os.environ.get(f"{self.name}_BUCKET", os.environ["BUCKET"])

    @property
    def folder(self):
        return os.environ.get(
            f"{self.name}_FOLDER", f"athena_audit/{self.name.lower()}"
        )


def run_query(query: str):
    client = boto3.client("athena")
    response = client.start_query_execution(
        QueryString=query,
        ResultConfiguration={
            "OutputLocation": f"s3://{get_athena_output_bucket()}/{get_athena_output_folder()}"
        },
        WorkGroup=get_workgroup(),
    )
    time.sleep(2)
    execution_id = response["QueryExecutionId"]
    sleep_in_interval = 10
    intervals = int(get_query_timeout() / sleep_in_interval)
    result = {"execution_id": execution_id}
    for wait_index in range(intervals):
        stats = client.get_query_execution(QueryExecutionId=execution_id)
        status = stats["QueryExecution"]["Status"]["State"]
        if status in ["FAILED", "CANCELLED", "SUCCEEDED"]:
            result["status"] = status
            if status == "SUCCEEDED":
                result["seconds"] = stats["QueryExecution"]["Statistics"][
                    "EngineExecutionTimeInMillis"
                ]
                result["data_scanned_mb"] = (
                    int(stats["QueryExecution"]["Statistics"]["DataScannedInBytes"])
                    / 1024.0
                    / 1024.0
                )
            if "StateChangeReason" in stats["QueryExecution"]["Status"]:
                error_message = stats["QueryExecution"]["Status"]["StateChangeReason"]
                result["error"] = error_message
            return result
        if wait_index % 6 == 0 and wait_index > 0:
            logger.info("Waiting to query for %d minutes", wait_index / 6)
        time.sleep(sleep_in_interval)
    err_msg = f"Timeout of {get_query_timeout()} seconds occurred. Canceling query execution. Query id: {execution_id}"
    logger.warning(err_msg)
    result["error"] = err_msg
    client.stop_query_execution(QueryExecutionId=execution_id)
    return result


def get_query_results(query: str) -> Generator[Dict, None, None]:
    result = run_query(query)
    client = boto3.client("athena")
    pages_it = client.get_paginator("get_query_results").paginate(
        QueryExecutionId=result["execution_id"]
    )
    columns = None
    for page in pages_it:
        row_it = iter(page["ResultSet"]["Rows"])
        if columns is None:
            header = next(row_it)["Data"]
            columns = [header[i]["VarCharValue"] for i in range(len(header))]
        for row in row_it:
            row_dict = {}
            for i, col in enumerate(columns):
                value = row["Data"][i]
                row_dict[col] = value["VarCharValue"] if value else None
            yield row_dict


def insert_data(full_day_str: str, region: str):
    year = full_day_str[:4]
    month = full_day_str[5:7]
    day = full_day_str[-2:]

    run_query(
        f"ALTER TABLE {TableType.HISTORY.table_name} "
        f"ADD IF NOT EXISTS PARTITION (region='{region}', day='{full_day_str}')"
    )

    alter_sql = f"""ALTER TABLE {TableType.CLOUD_TRAIL.table_name} ADD IF NOT EXISTS 
PARTITION (region= '{region}', year= '{year}', month= '{month}', day= '{day}') 
LOCATION 's3://{TableType.CLOUD_TRAIL.bucket}/{TableType.CLOUD_TRAIL.folder}/{region}/{year}/{month}/{day}/'"""
    run_query(alter_sql)

    insert_sql = f"""
INSERT INTO {TableType.EVENTS.table_name} (query_id, event_time, user_identity_type,
  user_identity_principal, user_identity_arn, user_agent,
  source_ip, query, database, data_scanned, workgroup, region, day)
SELECT 
  json_extract_scalar(responseelements, '$.queryExecutionId') AS query_id,
  CAST(From_iso8601_timestamp(eventtime) AS TIMESTAMP) AS event_time,
  useridentity.type AS user_identity_type, 
  useridentity.principalid AS user_identity_principal,
  useridentity.arn AS user_identity_arn,   
  useragent AS user_agent, 
  sourceipaddress AS source_ip,  
  h.query, 
  json_extract_scalar(requestParameters, '$.queryExecutionContext.database') AS database,
  data_scanned,
  json_extract_scalar(requestParameters, '$.queryExecutionContext.workGroup') AS workgroup,
  ct.region,
  '{full_day_str}' AS day
FROM {TableType.CLOUD_TRAIL.table_name} AS ct
     LEFT OUTER JOIN {TableType.HISTORY.table_name} AS h 
     ON json_extract_scalar(responseelements, '$.queryExecutionId') = query_id
        AND h.day = '{full_day_str}' AND h.region = '{region}'
WHERE eventsource = 'athena.amazonaws.com'
      AND eventname IN ('StartQueryExecution')
      AND ct.region = '{region}'
      AND year = '{year}'
      AND month = '{month}'
      AND ct.day = '{day}'
"""
    result = run_query(insert_sql)
    logger.info(f"Inserted data for {full_day_str}, region: {region}. Result: {result}")


def repair_events_table(days_back: int):
    sql = f"ALTER TABLE {TableType.EVENTS.table_name} ADD IF NOT EXISTS"
    for i in range(days_back, 0, -1):
        day = get_day_back(i)
        for region in get_regions():
            sql += f"\nPARTITION (region='{region}', day='{day}')"
    run_query(sql)


def tables_exist(tables: List[str]) -> bool:
    sql = "EXPLAIN "
    for idx, table in enumerate(tables):
        sql += f"(SELECT 1 FROM {table} LIMIT 1)"
        if idx < len(tables) - 1:
            sql += " UNION ALL "
    result = run_query(sql)
    return "error" not in result


def create_table(table_type: TableType):
    run_query(f"DROP TABLE IF EXISTS {table_type.table_name}")
    file_name = os.path.join(
        Path(os.path.dirname(os.path.abspath(__file__))).absolute(),
        f"resources/create_{table_type.name.lower()}_table.sql",
    )
    with open(file_name, "r") as file:
        sql = file.read()
    keywords = {
        "table": table_type.table_name,
        "bucket": table_type.bucket,
        "prefix": table_type.folder,
    }
    for key in keywords:
        sql = sql.replace(f"{{{key}}}", keywords[key])
    result = run_query(sql)
    logger.info(f"Table {table_type.table_name} created. Result: {result}")


def init_database(repair_days_back: int):
    if not tables_exist([t.table_name for t in TableType]):
        logger.info("Tables do not exist. Creating tables...")
        if get_db_name() != "default":
            run_query(f"CREATE DATABASE {get_db_name()}")
        for table_type in TableType:
            create_table(table_type)
        repair_events_table(repair_days_back)
        logger.info(f"Finished creating tables")
        return True
    return False


def lambda_handler(event, context):
    init_database(event.get("repair_days_back", 90))
    if "day" in event:
        from_day = event["day"]
        to_day = event["day"]
    else:
        from_day = event.get("from_day", get_yesterday())
        to_day = event.get("to_day", get_yesterday())

    validate_day_range(from_day, to_day)
    result = {}
    logger.info(f"START. from day: {from_day}, to day: {to_day}")
    regions = event["regions"].split(",") if "regions" in event else get_regions()
    for day in get_days(from_day, to_day):
        for region in regions:
            logger.info(f"Current region: {region}, day: {day}")
            clear_folder(
                TableType.EVENTS.bucket,
                f"{TableType.EVENTS.folder}/region={region}/day={day}",
            )
            insert_data(day, region)
    events_count = int(
        list(
            get_query_results(
                f"SELECT COUNT() AS events FROM {TableType.EVENTS.table_name} "
                f"WHERE day BETWEEN '{from_day}' AND '{to_day}'"
            )
        )[0]["events"]
    )

    logger.info(
        f"FINISH. from day: {from_day}, to day: {to_day}. Events: {events_count}"
    )
    result["from_day"] = from_day
    result["to_day"] = to_day
    result["events"] = events_count

    return result
