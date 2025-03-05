import logging
import os
from pathlib import Path

import pytest

from athena_history import lambda_handler, get_yesterday


def init_creds_from_file():
    abs_path = str(Path(os.path.dirname(os.path.abspath(__file__))).parent.absolute())
    file_name = os.path.join(abs_path, "aws", "config")
    if os.path.exists(file_name):
        os.environ["AWS_CONFIG_FILE"] = file_name
        logging.info("AWS config file set")


@pytest.fixture(autouse=True, scope="module")
def set_evn():
    init_creds_from_file()
    os.environ["BUCKET"] = "athena-audit-for-tests"
    os.environ["AWS_REGION"] = "us-east-1"


def test_default_params():
    result = lambda_handler({"workgroup": "primary", "force": True}, None)
    assert result["from_day"] == get_yesterday()
    assert result["to_day"] == get_yesterday()
    assert int(result["workgroups"]) >= 0
    assert int(result["data-exists-workgroups"]) >= 0
    assert int(result["records"]) >= 0
