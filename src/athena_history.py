import concurrent.futures
import gzip
import json
import logging
import os
import shutil
import tempfile
from datetime import date, datetime
from typing import List, Generator, Dict

import boto3

from common_utils import (
    get_days,
    clear_folder,
    obj_exists,
    get_yesterday,
)

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def get_bucket() -> str:
    return os.environ["BUCKET"]


def get_region():
    return os.environ["AWS_REGION"]


def get_location() -> str:
    location = os.environ.get("FOLDER", "athena_audit/history")
    return location[:-1] if location.endswith("/") else location


def get_daily_location(day: str) -> str:
    return f"{get_location()}/region={get_region()}/day={day}"


def get_daily_location_workgroup(day: str, workgroup: str) -> str:
    return f"{get_location()}/region={get_region()}/day={day}/workgroup={workgroup}"


def get_history_key(day: str, workgroup: str) -> str:
    return f"{get_daily_location_workgroup(day, workgroup)}/data.json.gz"


def create_history_days_range(
    from_day: str, to_day: str, workgroup: str = None, clear: bool = False
) -> Dict[str, any]:
    if clear:
        for day in get_days(from_day, to_day):
            if workgroup:
                path = get_daily_location_workgroup(day, workgroup)
            else:
                path = get_daily_location(day)
            clear_folder(get_bucket(), path)
    if workgroup is None:
        client = boto3.client("athena")
        workgroups: List[str] = [
            w["Name"] for w in client.list_work_groups()["WorkGroups"]
        ]
    else:
        workgroups = [workgroup]
    exists = 0
    total_records = 0
    for w in workgroups:
        key = get_history_key(from_day, w)
        data_exists = obj_exists(get_bucket(), key)
        logger.info(f"Current workgroup: {w}. Data Exists: {data_exists}")
        if data_exists:
            exists += 1
        else:
            records = create_history_day_for_workgroup(from_day, to_day, workgroup=w)
            logger.info(f"Queries for workgroup {w} written: {records}")
            total_records += records
    if exists > 0:
        logger.info(f"Data existed for {exists} workgroups")
    return {
        "from_day": from_day,
        "to_day": to_day,
        "workgroups": len(workgroups),
        "data-exists-workgroups": exists,
        "records": total_records,
    }


def get_query_exec_day(query_exe: dict) -> str:
    query_date = query_exe["Status"]["CompletionDateTime"]
    return query_date.strftime("%Y-%m-%d")


def get_query_executions_data(athena, ids: List[str]) -> dict:
    return athena.batch_get_query_execution(QueryExecutionIds=ids)


def get_query_executions_for_workgroup(
    workgroup: str, from_day: str
) -> Generator[dict, None, None]:
    athena = boto3.client("athena")
    max_workers = 3
    paginator = iter(
        athena.get_paginator("list_query_executions").paginate(WorkGroup=workgroup)
    )
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as threat_pool:
        while True:
            futures = []
            for _ in range(max_workers):
                try:
                    page = next(paginator)
                except StopIteration:
                    break
                if len(page["QueryExecutionIds"]) > 0:
                    futures.append(
                        threat_pool.submit(
                            get_query_executions_data, athena, page["QueryExecutionIds"]
                        )
                    )
            if len(futures) == 0:
                return
            # Wait for all futures to complete, in the same order they were created
            for future in futures:
                query_executions = future.result()
                for query in query_executions["QueryExecutions"]:
                    if query["Status"]["State"] in ["SUCCEEDED", "FAILED", "CANCELLED"]:
                        query_day = get_query_exec_day(query)
                        if query_day >= from_day:
                            yield query
                        else:
                            return


def upload_history_file(file_name: str, day: str, workgroup: str):
    with tempfile.NamedTemporaryFile(mode="wb", delete=False) as f_out:
        with (
            open(file_name, "rb") as json_file_in,
            gzip.open(f_out.name, "wb") as gzip_fie,
        ):
            # noinspection PyTypeChecker
            shutil.copyfileobj(json_file_in, gzip_fie)
        key = get_history_key(day, workgroup)
        s3_client = boto3.client("s3")
        s3_client.upload_file(f_out.name, get_bucket(), key)
    logger.info(f"uploaded key: {key}")


def create_history_day_for_workgroup(from_day: str, to_day: str, workgroup: str) -> int:
    current_day = to_day
    current_day_rows = 0
    total_rows = 0
    json_file = None

    for query in get_query_executions_for_workgroup(workgroup, from_day):
        query_day = get_query_exec_day(query)
        if query_day < current_day or query_day < from_day:
            if json_file:
                json_file.close()
                logger.info(f"Day: {current_day}, Total: {current_day_rows} rows")
                total_rows += current_day_rows
                current_day_rows = 0
                upload_history_file(json_file.name, current_day, workgroup)
                os.remove(json_file.name)
                json_file = None
                current_day = query_day
        if current_day == query_day:
            if json_file is None:
                json_file = tempfile.NamedTemporaryFile(mode="w", delete=False)
            if "Statistics" in query and "DataScannedInBytes" in query["Statistics"]:
                data_scanned = query["Statistics"]["DataScannedInBytes"]
            else:
                data_scanned = 0
            record = {
                "query_id": query["QueryExecutionId"],
                "query": query["Query"],
                "data_scanned": data_scanned,
                "workgroup": workgroup,
            }
            json_file.write(json.dumps(record))
            json_file.write("\n")
            current_day_rows += 1
            if current_day_rows % 1000 == 0:
                logger.info(f"Day: {current_day}, Written {current_day_rows} rows")
    if json_file:
        logger.info(f"Day: {current_day}, Total: {current_day_rows} rows")
        total_rows += current_day_rows
        json_file.close()
        upload_history_file(json_file.name, current_day, workgroup)
    return current_day_rows


def validate_day_range(from_day: str, to_day: str):
    if from_day > to_day:
        raise ValueError(f"from_day: {from_day} is greater than to_day: {to_day}")
    if (date.today() - datetime.strptime(from_day, "%Y-%m-%d").date()).days >= 45:
        raise ValueError(
            f"from_day: {from_day} is older than 45 days. Only 45 days are stored in Athena"
        )


def lambda_handler(event, context):
    if "day" in event:
        from_day = event["day"]
        to_day = event["day"]
    else:
        from_day = event.get("from_day", get_yesterday())
        to_day = event.get("to_day", get_yesterday())

    validate_day_range(from_day, to_day)

    logger.info(f"START. from day: {from_day}, to day: {to_day}")
    result = create_history_days_range(
        from_day, to_day, event.get("workgroup"), event.get("force", False)
    )
    logger.info(result)
    return result
