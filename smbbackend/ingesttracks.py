#########################################################################
#
# Copyright 2018, GeoSolutions Sas.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.
#
#########################################################################

"""Smb track ingestion

This module performs ingestion of the location points collected by the smb-app.

Workflow is something like:

-  Mobile app collects data and sends it to an AWS S3 bucket;
-  whenever data is stored in S3, a new message is pushed to AWS SNS, which
   asynchronously delivers it to subscribed consumers;
-  Some web app (such as the AWS gateway, or the smb-portal) exposes an
   endpoint that is subscribed to the AWS SNS' topic used by the mibile app to
   push the notification;
-  SNS issues a POST request to the web app's endpoint, sending a message
   that informs of new data present in S3;
-  At that point, the web app can call into this module's
   ``ingest_track`` function. This will take care of fetching the data
   and update the database

"""

from collections import namedtuple
import datetime as dt
import io
import logging
import os
import pathlib
import re
from typing import List
import zipfile

import boto3
import pytz

from ._constants import VehicleType
from .utils import get_query

logger = logging.getLogger(__name__)

_DATA_FIELDS = [
    "accelerationX",
    "accelerationY",
    "accelerationZ",
    "accuracy",
    "batConsumptionPerHour",
    "batteryLevel",
    "deviceBearing",
    "devicePitch",
    "deviceRoll",
    "elevation",
    "gps_bearing",
    "humidity",
    "latitude",
    "longitude",
    "lumen",
    "pressure",
    "proximity",
    "sessionId",
    "speed",
    "temperature",
    "timeStamp",
    "vehicleMode",
    "serialVersionUID",
]

PointData = namedtuple("PointData", _DATA_FIELDS)


def ingest_track(s3_bucket_name: str, object_key: str,
                 db_connection) -> int:
    """Ingest track data into smb database"""
    try:
        track_owner = get_track_owner_uuid(object_key)
    except AttributeError:
        raise RuntimeError(
            "Could not determine track owner for object {}".format(object_key))
    logger.debug("Retrieving data from S3 bucket...")
    raw_data = retrieve_track_data(s3_bucket_name, object_key)
    logger.debug("Parsing retrieved track data...")
    parsed_data = parse_track_data(raw_data)
    logger.debug("Performing calculations and creating database records...")
    with db_connection:  # changes are committed when `with` block exits
        with db_connection.cursor() as cursor:
            user_id = get_track_owner_internal_id(track_owner, cursor)
            track_id = insert_track(parsed_data, user_id, cursor)
            insert_collected_points(track_id, parsed_data, cursor)
            insert_segments(track_id, track_owner, cursor)
    return track_id


def insert_segments(track_id: int, owner_uuid: str, db_cursor):
    db_cursor.execute(
        get_query("insert-track-segments.sql"),
        {
            "user_uuid": owner_uuid,
            "track_id": track_id,
        }
    )
    segment_ids = db_cursor.fetchall()
    return [item[0] for item in segment_ids]


def insert_track(track_data: List[PointData], owner: str, db_cursor) -> int:
    """Insert track data into the main database"""
    session_id = list(set([pt.sessionId for pt in track_data]))[0]
    query = get_query("insert-track.sql")
    db_cursor.execute(
        query,
        (owner, session_id, dt.datetime.now(pytz.utc))
    )
    track_id = db_cursor.fetchone()[0]
    return track_id


def insert_collected_points(track_id: int, track_data: List[PointData],
                            db_cursor):
    query = get_query("insert-collectedpoint.sql")
    for pt in track_data:
        vehicle_type = _get_vehicle_type(int(pt.vehicleMode))
        db_cursor.execute(
            query,
            {
                "vehicle_type": vehicle_type.name,
                "track_id": track_id,
                "longitude": pt.longitude,
                "latitude": pt.latitude,
                "accelerationx": pt.accelerationX,
                "accelerationy": pt.accelerationY,
                "accelerationz": pt.accelerationZ,
                "accuracy": pt.accuracy,
                "batconsumptionperhour": pt.batConsumptionPerHour,
                "batterylevel": pt.batteryLevel,
                "devicebearing": pt.deviceBearing,
                "devicepitch": pt.devicePitch,
                "deviceroll": pt.deviceRoll,
                "elevation": pt.elevation,
                "gps_bearing": pt.gps_bearing,
                "humidity": pt.humidity,
                "lumen": pt.lumen,
                "pressure": pt.pressure,
                "proximity": pt.proximity,
                "speed": pt.speed,
                "temperature": pt.temperature,
                "sessionid": pt.sessionId,
                "timestamp": dt.datetime.fromtimestamp(
                    int(pt.timeStamp) / 1000,
                    pytz.utc
                ),
            }
        )


def retrieve_track_data(s3_bucket: str, object_key: str) -> str:
    """Download track data file from S3 and return the data"""
    s3 = boto3.resource("s3")
    obj = s3.Object(s3_bucket, object_key)
    response = obj.get()
    input_buffer = io.BytesIO(response["Body"].read())
    # TODO: do some integrity checks to the data
    result = ""
    with zipfile.ZipFile(input_buffer) as zip_handler:
        for member_name in zip_handler.namelist():
            result += zip_handler.read(member_name).decode("utf-8")
    return result


def parse_track_data(raw_track_data: str) -> List[PointData]:
    result = []
    for index, line in enumerate(raw_track_data.splitlines()):
        if index > 0 and line != "":  # ignoring first line, it is file header
            result.append(parse_track_data_line(line))
    return result


def parse_track_data_line(line: str) -> PointData:
    info = line.split(",")
    return PointData(*info[:len(_DATA_FIELDS)])


def get_track_owner_uuid(object_key: str) -> str:
    search_obj = re.search(r"cognito/smb/([\w-]{36})", object_key)
    return search_obj.group(1)


def get_track_owner_internal_id(keycloak_uuid: str, db_cursor):
    db_cursor.execute(
        "SELECT user_id FROM bossoidc_keycloak WHERE \"UID\" = %s",
        (keycloak_uuid,)
    )
    try:
        return db_cursor.fetchone()[0]
    except TypeError:
        raise RuntimeError("Could not determine track owner internal ID")


def _get_vehicle_type(raw_vehicle_type: int) -> VehicleType:
    """Return the vehicle type as used in the portal DB

    The mapping between vehicle types is based on the structure of the java
    enum used in the app's source code:

    https://github.com/geosolutions-it/smb-app/blob/\
        72b3a336f97c600fee9b63d63af46a955076f6e9/app/src/main/java/it/\
        geosolutions/savemybike/model/Vehicle.java#L14

    """

    try:
        vehicle_type = VehicleType(raw_vehicle_type)
    except ValueError:
        vehicle_type = VehicleType.unknown
    return vehicle_type
