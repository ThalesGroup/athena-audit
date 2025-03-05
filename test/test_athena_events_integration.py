import gzip
import logging
import os
import shutil
import tempfile
from pathlib import Path

import boto3
import pytest

from athena_events import (
    tables_exist,
    get_query_results,
    run_query,
    lambda_handler,
    TableType,
)


def init_creds_from_file():
    abs_path = str(Path(os.path.dirname(os.path.abspath(__file__))).parent.absolute())
    file_name = os.path.join(abs_path, "aws", "config")
    if os.path.exists(file_name):
        os.environ["AWS_CONFIG_FILE"] = file_name
        logging.info("AWS config file set")


def get_file_resource(file_name: str) -> str:
    abs_path = str(Path(os.path.dirname(os.path.abspath(__file__))).absolute())
    return os.path.join(abs_path, "resources", file_name)


@pytest.fixture(autouse=True, scope="module")
def set_evn():
    init_creds_from_file()
    os.environ["BUCKET"] = "athena-audit-for-tests"
    os.environ["AWS_REGION"] = "us-east-1"
    os.environ["CLOUD_TRAIL_FOLDER"] = "temp/athena-audit/CloudTrail"
    os.environ["EVENTS_FOLDER"] = "temp/athena_audit/events"
    os.environ["HISTORY_FOLDER"] = "temp/athena_audit/history"
    os.environ["CLOUD_TRAIL_BUCKET"] = os.environ["BUCKET"]


def test_tables_exists():
    assert tables_exist(["table_which_does_not_exist"]) == False
    assert not tables_exist(
        ["table_which_does_not_exist1", "table_which_does_not_exist2"]
    )
    assert tables_exist(["information_schema.tables", "information_schema.columns"])
    assert not tables_exist(["information_schema.tables", "table_which_does_not_exist"])


def test_run_query():
    result = run_query("SELECT 1")
    assert result["status"] == "SUCCEEDED", result
    result = run_query("SELECT 1 FROM table_which_does_not_exist")
    assert result["status"] == "FAILED", result
    assert "TABLE_NOT_FOUND" in result["error"], result


def test_get_query_results():
    result = get_query_results(
        "(SELECT 1 AS col) UNION ALL (SELECT 2 AS col) ORDER BY col"
    )
    assert list(result) == [{"col": "1"}, {"col": "2"}]


def test_init_database():
    from athena_events import init_database, TableType, run_query

    run_query(f"DROP TABLE IF EXISTS {TableType.CLOUD_TRAIL.table_name}")
    assert init_database(1)


def test_full_flow_empty_data():
    result = lambda_handler({}, None)
    assert result["events"] == 0


def _upload_test_event(day: str):
    year, month, day_num = day[:4], day[5:7], day[8:]
    s3 = boto3.client("s3")
    s3.upload_file(
        get_file_resource("example_event.json"),
        os.environ["CLOUD_TRAIL_BUCKET"],
        f"{os.environ['CLOUD_TRAIL_FOLDER']}/{os.environ['AWS_REGION']}/{year}/{month}/{day_num}/data.json",
    )


def test_full_flow():
    day = "2024-03-02"
    _upload_test_event(day)
    result = lambda_handler({"day": day}, None)
    assert result["events"] == 1


def test_full_flow_with_history():
    day = "2024-03-03"
    _upload_test_event(day)
    with tempfile.NamedTemporaryFile(mode="wb", delete=False) as f_out:
        with (
            open(
                get_file_resource("athena_history_example.jsonl"), "rb"
            ) as json_file_in,
            gzip.open(f_out.name, "wb") as gzip_fie,
        ):
            # noinspection PyTypeChecker
            shutil.copyfileobj(json_file_in, gzip_fie)
        key = f"{os.environ['HISTORY_FOLDER']}/region={os.environ['AWS_REGION']}/day={day}/data.jsonl.gz"
        s3 = boto3.client("s3")
        s3.upload_file(f_out.name, os.environ["BUCKET"], key)

    result = lambda_handler({"day": day}, None)
    assert result["events"] == 1
    result = list(
        get_query_results(
            f"SELECT day, region, query_id, user_identity_principal, query, data_scanned "
            f"FROM {TableType.EVENTS.table_name} "
            f"WHERE day = '{day}' AND region = '{os.environ['AWS_REGION']}'"
        )
    )
    assert len(result) == 1
    assert result[0] == {
        "data_scanned": "100",
        "day": "2024-03-03",
        "query": "SELECT 1",
        "query_id": "cc405a40-434f-41f7-9f20-9a06edd85b45",
        "region": "us-east-1",
        "user_identity_principal": "SOME_USER",
    }
