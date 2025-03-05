import logging
from datetime import datetime, timedelta, date
from typing import Generator

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger()


def get_day_back(back: int) -> str:
    return str(date.today() - timedelta(back))


def get_yesterday() -> str:
    return get_day_back(1)


def get_days(from_day: str, to_day: str) -> Generator[str, None, None]:
    date_object = datetime.strptime(from_day, "%Y-%m-%d").date()
    while True:
        date_str = str(date_object)[:10]
        yield date_str
        if date_str == to_day:
            break
        date_object += timedelta(1)


def obj_exists(bucket: str, key: str):
    s3_client = boto3.client("s3")
    try:
        s3_client.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError as e:
        if e.response["Error"]["Code"] == "404":
            return False
        else:
            raise e


def clear_folder(bucket: str, s3_folder: str) -> int:
    s3 = boto3.resource("s3")
    bucket = s3.Bucket(bucket)
    res = bucket.objects.filter(Prefix=s3_folder).delete()
    deleted = 0 if len(res) == 0 else len(res[0]["Deleted"])
    logger.info(f"{deleted} objects deleted from under {s3_folder}")
    return deleted
