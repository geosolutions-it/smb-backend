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
from collections import namedtuple
import datetime as dt
from enum import Enum
import logging
import os
import pathlib

import psycopg2

logger = logging.getLogger(__name__)


TrackInfo = namedtuple("TrackInfo", [
    "id",
    "created_at",
    "owner_id",
    "aggregated_costs",
    "aggregated_emissions",
    "aggregated_health",
    "duration",
    "start_date",
    "end_date",
    "length",
    "is_valid",
    "validation_error",
    "segments",
])


class MessageType(Enum):
    """Message types that are recognized by the various smb components"""

    device_registered = 1
    s3_received_track = 2
    track_uploaded = 3
    track_validated = 4
    indexes_have_been_calculated = 5
    badges_have_been_updated = 6
    badge_won = 7
    competitions_have_been_updated = 8
    prize_won = 9
    unknown = 10
    bike_observed = 11


class ValidationError(Enum):
    segment_speed_too_high = 1
    segment_length_too_big = 2
    segment_duration_too_long = 3



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


def get_track_info(track_id, db_cursor) -> TrackInfo:
    db_cursor.execute(
        get_query("select-track.sql"),
        {"track_id": track_id}
    )
    row = db_cursor.fetchone()
    if row is not None:
        return TrackInfo(*row)
    else:
        raise RuntimeError("Invalid track id: {!r}".format(track_id))


def update_track_info(track_id, db_cursor):
    db_cursor.execute(
        get_query("update-track-info.sql"),
        {"track_id": track_id}
    )

def get_user_uuid(user_id: int, db_cursor):
    db_cursor.execute(
        "SELECT \"UID\" FROM bossoidc_keycloak WHERE user_id = %(user_id)s",
        {"user_id": user_id}
    )
    result = db_cursor.fetchone()
    return result[0] if result is not None else None