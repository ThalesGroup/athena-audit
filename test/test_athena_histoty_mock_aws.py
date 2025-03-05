import os
from typing import List

import boto3.session
import pytest
from moto import mock_aws

from athena_history import lambda_handler
from common_utils import get_day_back


# This function is used to mock the response of the get_query_executions_data function since the
# batch_get_query_execution is not implemented in moto
def _get_query_executions_data(athena_client, ids: List[str]) -> dict:
    result = []
    for query_id in ids:
        result.append(
            athena_client.get_query_execution(QueryExecutionId=query_id)[
                "QueryExecution"
            ]
        )
    return {"QueryExecutions": result}


@pytest.fixture(autouse=True)
def athena_mock(monkeypatch):
    monkeypatch.setenv("BUCKET", "my-bucket")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
    monkeypatch.setenv("AWS_REGION", "us-east-1")
    monkeypatch.setattr(
        "athena_history.get_query_executions_data", _get_query_executions_data
    )
    mock = mock_aws()
    mock.start()
    session = boto3.session.Session()
    s3 = session.client("s3")
    s3.create_bucket(Bucket=os.environ["BUCKET"])
    yield
    mock.stop()


def _run_queries(workgroup: str, num: int):
    athena = boto3.client("athena")
    queries = 100
    for _ in range(queries):
        athena.start_query_execution(
            QueryString="SELECT 1",
            ResultConfiguration={
                "OutputLocation": f"s3://{os.environ["BUCKET"]}/temp/athena"
            },
            WorkGroup=workgroup,
        )


def test_validate_default_flow(monkeypatch):
    _run_queries("primary", 100)
    result = lambda_handler({"day": get_day_back(0), "force": True}, None)
    assert result == {
        "data-exists-workgroups": 0,
        "from_day": "2025-03-05",
        "records": 100,
        "to_day": "2025-03-05",
        "workgroups": 1,
    }


def test_validate_default_flow_multiple_workgroups(monkeypatch):
    athena = boto3.client("athena")
    for i in range(10):
        workgroup = f"workgroup-{i}"
        athena.create_work_group(Name=workgroup, Configuration={})
        _run_queries(workgroup, 100)

    result = lambda_handler({"day": get_day_back(0), "force": True}, None)
    assert result == {
        "data-exists-workgroups": 0,
        "from_day": "2025-03-05",
        "records": 1000,
        "to_day": "2025-03-05",
        "workgroups": 11,
    }
