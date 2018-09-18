#########################################################################
#
# Copyright 2018, GeoSolutions Sas.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.
#
#########################################################################

import calendar
import datetime as dt
from functools import wraps
import json
import logging
import os
import pathlib

import boto3
import psycopg2

logger = logging.getLogger(__name__)


def extract_sns_message(event: dict) -> dict:
    try:
        raw_sns_message = event["Records"][0]["Sns"]["Message"]
    except (KeyError, IndexError):
        raise RuntimeError("Unsupported event")
    else:
        return json.loads(raw_sns_message)


def handler(handler_func):
    setup_logging()

    @wraps(handler_func)
    def inner(*args, **kwargs):
        return handler_func(*args, **kwargs)

    return inner


def get_db_connection(dbname, user, password, host="localhost", port="5432"):
    return psycopg2.connect(
        host=host,
        port=port,
        dbname=dbname,
        user=user,
        password=password
    )


def get_query(filename) -> str:
    base_dir = pathlib.Path(os.path.abspath(__file__)).parent
    query_path = base_dir / "sqlqueries" / filename
    with query_path.open(encoding="utf-8") as fh:
        query = fh.read()
    return query


def get_week_bounds(day: dt.datetime):
    index_day_of_week = calendar.weekday(day.year, day.month, day.day)
    first_weekday = day - dt.timedelta(days=index_day_of_week+1)
    last_weekday = day + dt.timedelta(days=5-index_day_of_week)
    first = first_weekday.replace(hour=0, minute=0, second=0, microsecond=0)
    last = last_weekday.replace(hour=23, minute=59, second=59,
                                microsecond=9999)
    return first, last


def publish_message(topic_arn: str, **kwargs):
    sns_client = boto3.client("sns")
    message = {
        "default": ", ".join(
            ["{}: {}".format(str(k), str(v)) for k, v in kwargs.items()]),
        "lambda": json.dumps(kwargs.copy())
    }
    publish_response = sns_client.publish(
        TopicArn=topic_arn,
        Message=json.dumps(message),
        MessageStructure="json"
    )
    if publish_response["ResponseMetadata"]["HTTPStatusCode"] != 200:
        raise RuntimeError(
            "Could not publish `track created` message to "
            "SNS: {}".format(publish_response)
        )


def setup_logging():
    log_level = os.getenv("LOG_LEVEL", "WARNING")
    log_level = getattr(logging, log_level.upper())
    logging.basicConfig(level=log_level)
    to_disable = [
        "botocore",
    ]
    for log_name in to_disable:
        logging.getLogger(log_name).propagate = False
