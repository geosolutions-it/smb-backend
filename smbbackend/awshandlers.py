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
import re
from typing import List
from typing import Tuple

from pyfcm import FCMNotification

from . import calculateindexes
from . import calculateprizes
from .exceptions import NonRecoverableError
from . import processor
from . import notifications
from . import updatebadges
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
FCM_PUSH_SERVICE = FCMNotification(api_key=os.getenv("FCM_SERVER_KEY"))


def update_competitions(notify_completion=True):
    """Handler for periodically updating competitions"""
    _setup_logging()
    with _get_db_connection() as connection:
        with connection.cursor() as cursor:
            competition_results = calculateprizes.calculate_prizes(cursor)
    if notify_completion:
        with _get_db_connection() as connection:
            with connection.cursor() as cursor:
                _send_notification(MessageType.competitions_have_been_updated)
                _notify_competition_winners(competition_results, cursor)


# FIXME - send notification after the db_connection `with` block has ended
def aws_track_handler(event: dict, context):
    """Handler for lambda invocations

    This handler is called whenever an SNS notification is published on the
    relevant topic

    """

    _setup_logging()
    message = _extract_sns_message(event)
    message_type, message_arguments = _parse_message(message)
    logger.info("message_type: {}".format(message_type))
    logger.info("message_arguments: {}".format(message_arguments))
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
        bucket_name, object_key, owner_uuid = get_new_track_info(
            db_cursor, notify_completion=notify, **message_arguments)
        track_id, is_valid = ingest_track(
            db_cursor, bucket_name, object_key, owner_uuid,
            notify_completion=notify
        )
        logger.debug(f"track_id: {track_id} - is_valid: {is_valid}")
        if is_valid:
            calculate_indexes(db_cursor, track_id, owner_uuid,
                              notify_completion=notify)
            update_badges(db_cursor, track_id, owner_uuid,
                          notify_completion=notify)
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
        MessageType.s3_received_track: get_new_track_info,
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


def get_new_track_info(db_cursor, bucket_name, object_key, **kwargs):
    """Forward S3 message to both SNS and mobile apps.

    This function grabs the notification sent by S3 and passes it through our
    own wrapper, which is able to relay notifications to S3 and also to mobile
    apps via Firebase Cloud Messaging.

    """
    try:
        search_obj = re.search(r"[\w-]{36}", object_key)
        owner_uuid = search_obj.group()
    except AttributeError:
        raise NonRecoverableError(
            "Could not determine track owner for object {}".format(object_key))
    _send_notification(
        MessageType.track_uploaded,
        message_payload={
            "bucket_name": bucket_name,
            "object_key": object_key,
            "owner_uuid": owner_uuid,
        }
    )
    return bucket_name, object_key, owner_uuid


def ingest_track(db_cursor, bucket_name, object_key, owner_uuid,
                 notify_completion=True, **kwargs) -> Tuple[int, bool]:
    try:
        segments_data, track_id, session_id = processor.ingest_s3_data(
            s3_bucket_name=bucket_name,
            object_key=object_key,
            owner_uuid=owner_uuid,
            db_cursor=db_cursor
        )
        is_valid = processor.is_track_valid(segments_data)
        validation_errors = [s[2] for s in segments_data]
        flattened_errors = _flatten_validation_errors(validation_errors)
    except NonRecoverableError as exc:
        logger.exception("Could not perform track ingestion")
        track_id = None
        session_id = None
        is_valid = False
        validation_errors = [[{"message": exc.args[0]}]]
        flattened_errors = validation_errors[0][0]["message"]
    if notify_completion:
        _send_notification(
            MessageType.track_validated,
            message_payload={
                "user_uuid": owner_uuid,
                "track_id": track_id,
                "session_id": session_id,
                "is_valid": is_valid,
                "validation_errors": flattened_errors
            },
            use_fcm=True,
            fcm_devices={
                owner_uuid: get_user_active_devices(db_cursor, owner_uuid)
            }
        )
    return track_id, is_valid


def calculate_indexes(db_cursor, track_id, owner_uuid,
                      notify_completion=True, **kwargs):
    track_info = utils.get_track_info(track_id, db_cursor)
    if not track_info.is_valid:
        logger.debug(
            "Track {} is not valid, aborting...".format(track_id))
    else:
        calculateindexes.calculate_indexes(track_id, db_cursor)
        if notify_completion:
            _send_notification(
                MessageType.indexes_have_been_calculated,
                message_payload={
                    "track_id": track_id
                },
                use_fcm=True,
                fcm_devices={
                    owner_uuid: get_user_active_devices(db_cursor, owner_uuid)
                }
            )


def update_badges(db_cursor, track_id, owner_uuid,
                  notify_completion=True, **kwargs):
    track_info = utils.get_track_info(track_id, db_cursor)
    if not track_info.is_valid:
        logger.debug(
            "Track {} is not valid, aborting...".format(track_id))
    else:
        awarded_badges = updatebadges.update_badges(track_id, db_cursor)
        if notify_completion:
            _send_notification(
                MessageType.badges_have_been_updated,
                message_payload={
                    "track_id": track_id
                }
            )
            for badge in awarded_badges:
                _send_notification(
                    MessageType.badge_won,
                    message_payload={"badge_name": badge.name},
                    use_fcm=True,
                    fcm_devices={
                        owner_uuid: get_user_active_devices(
                            db_cursor, owner_uuid)
                    }
                )


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


def _parse_message(message: dict):
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


def _send_notification(message_type, message_payload=None,
                       use_sns=True, use_fcm=False, fcm_devices=None):
    logger.debug("inside _send_notification: {}".format(locals()))
    fcm_devices = dict(fcm_devices) if fcm_devices is not None else {}
    payload = dict(message_payload) if message_payload is not None else {}
    if use_sns:
        notifications.publish_message_to_sns(
            SNS_TOPIC, message_type, **payload)
    if use_fcm:
        for owner_uuid, device_ids in fcm_devices.items():
            payload["user"] = owner_uuid
            devices = list(device_ids) if device_ids else []
            notifications.publish_message_to_fcm(
                FCM_PUSH_SERVICE, devices, message_type, payload
            )


def get_user_active_devices(db_cursor, user_uuid):
    db_cursor.execute(
        utils.get_query("select-user-active-devices.sql"),
        {"owner_uuid": user_uuid}
    )
    return [row[0] for row in db_cursor.fetchall()]


def _flatten_validation_errors(errors: List[List[dict]]):
    flattened_errors = ""
    for segment_errors in errors:
        for error in segment_errors:
            flattened_error = "{} ({}: {} - {})".format(
                error["msg"], error["variable"], error["value"],
                error["vehicle_type"]
            )
            flattened_errors = ",".join((flattened_errors, flattened_error))
    return flattened_errors


def _setup_logging():
    log_level = getattr(logging, os.getenv("LOG_LEVEL", "WARNING").upper())
    logging.basicConfig(level=log_level)
    to_disable = [
        "botocore",
    ]
    for log_name in to_disable:
        logging.getLogger(log_name).propagate = False


def _notify_competition_winners(competition_results, db_cursor):
    for competition_info, winners in competition_results:
        for winner in winners:
            user_id = winner["user"]
            user_uuid = utils.get_user_uuid(user_id, db_cursor)
            prize_names = calculateprizes.get_prize_names(
                competition_info.id, winner["rank"], db_cursor)
            devices = get_user_active_devices(db_cursor, user_uuid)
            for prize_name in prize_names:
                _send_notification(
                    MessageType.prize_won,
                    message_payload={
                        "prize_name": prize_name
                    },
                    use_fcm=True,
                    fcm_devices=devices
                )
