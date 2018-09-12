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

import os

import boto3

from .ingesttracks import ingest_track
from . import utils

DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
SNS_TRACK_CREATED_TOPIC = os.getenv("SNS_TRACK_CREATED_TOPIC")


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
        aws_session = boto3.session.Session(region_name="us-west-2")
        utils.publish_message(
            str(track_id), SNS_TRACK_CREATED_TOPIC, aws_session)


def _get_db_connection():
    return utils.get_db_connection(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT,
    )


def handle_calculate_indexes(event: dict, context):
    message = utils.extract_sns_message(event)
    print("message: {}".format(message))
