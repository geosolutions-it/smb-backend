#########################################################################
#
# Copyright 2018, GeoSolutions Sas.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.
#
#########################################################################

"""AWS lambda handlers"""

import json
import logging
import os

import boto3

from . import calculateindexes
from . import processor
from . import updatebadges
from . import calculateprizes
from .exceptions import NonRecoverableError
from . import utils
from .utils import MessageType

logger = logging.getLogger(__name__)

DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
SNS_TOPIC = os.getenv("SNS_TOPIC")
USE_SYNCHRONOUS_EXECUTION = os.getenv("SYNCHRONOUS_EXECUTION", "").lower()


def aws_track_handler(event: dict, context):
    """Handler for lambda invocations

    This handler is called whenever an SNS notification is published on the
    relevant topic

    """

    _setup_logging()
    message = _extract_sns_message(event)
    message_type, message_arguments = _parse_message(message)
    logger.info("message_type: {}".format(message_type))
    logger.info("message_arguments: {}".format(message_type))
    if USE_SYNCHRONOUS_EXECUTION in ["true", "1", "yes"]:
        handler = compact_track_handler
    else:
        handler = modular_track_handler
    logger.info("handler: {}".format(handler))
    with _get_db_connection() as connection:
        with connection.cursor() as cursor:
            return handler(message_type, message_arguments, cursor)


def compact_track_handler(message_type:MessageType, message_arguments: dict,
                          db_cursor, notify=True):
    """Handler for track-related stuff that does everything"""

    if message_type == MessageType.s3_received_track:
        notify_new_track_available(
            notify_completion=notify, **message_arguments)
        track_id, validation_errors = ingest_track(
            db_cursor, notify_completion=notify, **message_arguments)
        is_valid = len(validation_errors) == 0
        if len(validation_errors) == 0:
            calculate_indexes(db_cursor, track_id, notify_completion=notify)
            update_badges(db_cursor, track_id, notify_completion=notify)
    else:
        logger.info("Ignoring message {!r}...".format(message_type.name))


def modular_track_handler(message_type:MessageType, message_arguments: dict,
                          db_cursor):
    """Handler for track-related stuff that does a single task

    When the execution of each task is completed, a new SNS message is
    published. This message is asynchronously pcked up by the lambda again
    in order to execute the next task.

    """

    notify = True
    handler = {
        MessageType.s3_received_track: notify_new_track_available,
        MessageType.track_uploaded: ingest_track,
        MessageType.track_validated: calculate_indexes,
        MessageType.indexes_have_been_calculated: update_badges,
        MessageType.badges_have_been_updated: None  # a handler to notify the app
    }.get(message_type)
    logger.info("message_type: {}".format(message_type))
    logger.info("handler: {}".format(handler))
    if handler is not None:
        handler(db_cursor, notify_completion=notify, **message_arguments)
    else:
        logger.info(
            "Could not handle message of type {!r}".format(message_type.name))


def notify_new_track_available(notify_completion=True, db_cursor=None,
                               **message_arguments):
    """Forward S3 message to both SNS and mobile apps.

    This function grabs the notification sent by S3 and passes it through our
    own wrapper, which is able to relay notifications to S3 and also to mobile
    apps via Firebase Cloud Messaging.

    """

    _publish_message(
        SNS_TOPIC, MessageType.track_uploaded, **message_arguments)


# TODO: Send a notification for all winners of all closed competitions
def update_competitions(db_cursor, notify_completion=True):
    calculateprizes.calculate_prizes(db_cursor)
    if notify_completion:
        _publish_message(SNS_TOPIC, MessageType.competitions_have_been_updated)


def ingest_track(db_cursor, bucket_name, object_key, notify_completion=True,
                 **kwargs):
    try:
        track_id, validation_errors = processor.ingest_s3_data(
            s3_bucket_name=bucket_name,
            object_key=object_key,
            db_cursor=db_cursor
        )
    except NonRecoverableError as exc:
        logger.exception("Could not perform track ingestion")
        track_id = None
        validation_errors = [{"message": exc.args[0]}]
    if notify_completion:
        _publish_message(
            SNS_TOPIC, MessageType.track_validated,
            track_id=track_id,
            is_valid=True if len(validation_errors) == 0 else False,
            validation_errors=validation_errors
        )
    return track_id, validation_errors


def calculate_indexes(db_cursor, track_id, notify_completion=True, **kwargs):
    track_info = utils.get_track_info(track_id, db_cursor)
    if not track_info.is_valid:
        logger.debug(
            "Track {} is not valid, aborting...".format(track_id))
    else:
        calculateindexes.calculate_indexes(track_id, db_cursor)
        if notify_completion:
            _publish_message(
                SNS_TOPIC,
                MessageType.indexes_have_been_calculated,
                track_id=track_id
            )


def update_badges(db_cursor, track_id, notify_completion=True, **kwargs):
    track_info = utils.get_track_info(track_id, db_cursor)
    if not track_info.is_valid:
        logger.debug(
            "Track {} is not valid, aborting...".format(track_id))
    else:
        awarded_badges = updatebadges.update_badges(track_id, db_cursor)
        if notify_completion:
            _publish_message(
                SNS_TOPIC, MessageType.badges_have_been_updated,
                track_id=track_id
            )
            for badge_name in awarded_badges:
                _publish_message(
                    SNS_TOPIC, MessageType.badge_won, badge_name=badge_name)


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
        message_type = MessageType.s3_received_track
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