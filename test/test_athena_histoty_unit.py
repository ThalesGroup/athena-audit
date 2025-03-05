import pytest

from athena_history import lambda_handler, get_location


def test_validate_day_range():
    with pytest.raises(ValueError, match="from_day.* is greater than to_day"):
        lambda_handler({"from_day": "2021-01-01", "to_day": "2020-01-01"}, None)
    with pytest.raises(ValueError, match="older than 45 days"):
        lambda_handler({"day": "2021-01-01"}, None)


def test_get_location():
    assert get_location() == "athena_audit/history"


def test_get_location_env_var(monkeypatch):
    monkeypatch.setenv("FOLDER", "my_location/")
    assert get_location() == "my_location"
