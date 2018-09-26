#########################################################################
#
# Copyright 2018, GeoSolutions Sas.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.
#
#########################################################################

"""AWS lambda handler for the `smbbackend.ingesttracks:ingest_tracks function
"""

from enum import Enum
import json
import logging
import os

import boto3

from .ingesttracks import ingest_track
from .calculateindexes import calculate_indexes
from .updatebadges import update_badges
from . import utils

logger = logging.getLogger(__name__)

DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
SNS_TOPIC = os.getenv("SNS_TOPIC")
USE_SYNCHRONOUS_EXECUTION = os.getenv("SYNCHRONOUS_EXECUTION", "").lower()


class MessageType(Enum):
    track_points_saved = 1
    track_ingested = 2
    indexes_calculated = 3
    badges_updated = 4
    unknown = 5


def aws_lambda_handler(event: dict, context):
    """Handler for AWS lambda invocations"""
    _setup_logging()
    message = _extract_sns_message(event)
    message_type, message_arguments = _parse_message(message)
    logger.info("message_type: {}".format(message_type))
    logger.info("message_arguments: {}".format(message_type))
    if USE_SYNCHRONOUS_EXECUTION in ["true", "1", "yes"]:
        handler = compact_handler
    else:
        handler = modular_handler
    logger.info("handler: {}".format(handler))
    return handler(message_type, message_arguments)


def compact_handler(message_type:MessageType, message_arguments: dict,
                    notify=False):
    """Handler for AWS lambda invocations that does everything"""
    if message_type == MessageType.track_points_saved:
        track_id = handle_track_ingestion(notify_completion=notify,
                                          **message_arguments)
        handle_indexes_calculations(track_id, notify_completion=notify)
        handle_badges_update(track_id, notify_completion=notify)
    else:
        logger.info("Ignoring message {!r}...".format(message_type.name))


def modular_handler(message_type:MessageType, message_arguments: dict):
    """Handler for AWS lambda invocations that does a single task

    When the execution of each task is completed, a new SNS message is
    published. This message is asynchronously pcked up by the lambda again
    in order to execute the next task.

    """

    notify = True
    handler = {
        MessageType.track_points_saved: handle_track_ingestion,
        MessageType.track_ingested: handle_indexes_calculations,
        MessageType.indexes_calculated: handle_badges_update,
        MessageType.badges_updated: None  # a handler to notify the app
    }.get(message_type)
    logger.info("message_type: {}".format(message_type))
    logger.info("handler: {}".format(handler))
    if handler is not None:
        handler(notify_completion=notify, **message_arguments)
    else:
        logger.info(
            "Could not handle message of type {!r}".format(message_type.name))


def handle_track_ingestion(bucket_name, object_key, notify_completion=True):
    track_id = ingest_track(
        s3_bucket_name=bucket_name,
        object_key=object_key,
        db_connection=_get_db_connection()
    )
    if notify_completion:
        _publish_message(
            SNS_TOPIC, MessageType.track_ingested, track_id=track_id)
    return track_id


def handle_indexes_calculations(track_id, notify_completion=True):
    calculate_indexes(
        track_id,
        _get_db_connection()
    )
    if notify_completion:
        _publish_message(
            SNS_TOPIC, MessageType.indexes_calculated, track_id=track_id)


def handle_badges_update(track_id, notify_completion=True):
    db_connection = _get_db_connection()
    update_badges(track_id, db_connection)
    # update_prizes(track_id, db_connection)
    if notify_completion:
        _publish_message(
            SNS_TOPIC, MessageType.badges_updated, track_id=track_id)


def _extract_sns_message(event: dict) -> dict:
    try:
        raw_sns_message = event["Records"][0]["Sns"]["Message"]
    except (KeyError, IndexError):
        raise RuntimeError("Unsupported event")
    else:
        return json.loads(raw_sns_message)


def _get_db_connection():
    return utils.get_db_connection(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT,
    )


def _parse_message(message):
    s3_info = message.get("Records", [{}])[0].get("s3")
    if s3_info:
        message_type = MessageType.track_points_saved
        try:
            message_arguments = {
                "bucket_name": s3_info["bucket"]["name"],
                "object_key": s3_info["object"]["key"]
            }
        except KeyError:
            raise RuntimeError("Invalid S3 message")
    else:
        msg = message.copy()
        type_ = msg.pop("message_type", MessageType.unknown.name).lower()
        message_type = MessageType[type_]
        message_arguments = msg
    return message_type, message_arguments


def _publish_message(topic_arn: str, message_type: MessageType, **kwargs):
    sns_client = boto3.client("sns")
    payload = kwargs.copy()
    payload["message_type"] = message_type.name
    message = {
        "default": ", ".join(
            ["{}: {}".format(str(k), str(v)) for k, v in payload.items()]),
        "lambda": json.dumps(payload)
    }
    publish_response = sns_client.publish(
        TopicArn=topic_arn,
        Message=json.dumps(message),
        MessageStructure="json"
    )
    if publish_response["ResponseMetadata"]["HTTPStatusCode"] != 200:
        raise RuntimeError("Could not publish {!r} message to SNS: {}".format(
            message_type.name, publish_response))


def _setup_logging():
    log_level = getattr(logging, os.getenv("LOG_LEVEL", "WARNING").upper())
    logging.basicConfig(level=log_level)
    to_disable = [
        "botocore",
    ]
    for log_name in to_disable:
        logging.getLogger(log_name).propagate = False