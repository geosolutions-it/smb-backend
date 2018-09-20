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

import logging
import os

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
SNS_TRACK_CREATED_TOPIC = os.getenv("SNS_TRACK_CREATED_TOPIC")
SNS_TRACK_INDEXES_CREATED_TOPIC = os.getenv("SNS_TRACK_INDEXES_CREATED_TOPIC")
SNS_BADGES_UPDATED_TOPIC = os.getenv("SNS_BADGES_UPDATED_TOPIC")


@utils.handler
def handle_ingest_track(event: dict, context):
    message = utils.extract_sns_message(event)
    try:
        s3_info = message["Records"][0]["s3"]
        bucket = s3_info["bucket"]["name"]
        object_key = s3_info["object"]["key"]
    except (KeyError, IndexError):
        raise RuntimeError("Invalid SNS message")
    else:
        db_connection = _get_db_connection()
        print("db_connection: {}".format(db_connection))
        track_id = ingest_track(
            s3_bucket_name=bucket,
            object_key=object_key,
            db_connection=db_connection
        )
        utils.publish_message(
            SNS_TRACK_CREATED_TOPIC, track_id=track_id)


@utils.handler
def handle_calculate_indexes(event: dict, context):
    message = utils.extract_sns_message(event)
    track_id = _get_track_id(message)
    db_connection = _get_db_connection()
    calculate_indexes(track_id, db_connection)
    utils.publish_message(SNS_TRACK_INDEXES_CREATED_TOPIC, track_id=track_id)


@utils.handler
def handle_update_badges(event: dict, context):
    message = utils.extract_sns_message(event)
    track_id = _get_track_id(message)
    print("track_id: {}".format(track_id))
    db_connection = _get_db_connection()
    update_badges(track_id, db_connection)
    utils.publish_message(SNS_BADGES_UPDATED_TOPIC, track_id=track_id)


@utils.handler
def handle_calculate_prizes(event: dict, context):
    message = utils.extract_sns_message(event)
    track_id = _get_track_id(message)
    print("track_id: {}".format(track_id))


def _get_db_connection():
    return utils.get_db_connection(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT,
    )


def _get_track_id(sns_message:dict):
    try:
        return sns_message["track_id"]
    except KeyError:
        raise RuntimeError("Invalid SNS message")
