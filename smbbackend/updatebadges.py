#########################################################################
#
# Copyright 2018, GeoSolutions Sas.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.
#
#########################################################################

"""Updating of badges when tracks are created/removed"""

from collections import namedtuple
import datetime as dt
import logging
from typing import List

import pytz

from .utils import get_query
from .utils import get_week_bounds
from ._constants import VehicleType

logger = logging.getLogger(__name__)


def update_badges(track_id: int, db_connection):
    badge_handlers = {
        "biker_level1": handle_biker_badge,
        "biker_level2": handle_biker_badge,
        "biker_level3": handle_biker_badge,
        "bike_surfer_level1": handle_bike_surfer_badge,
        "bike_surfer_level2": handle_bike_surfer_badge,
        "bike_surfer_level3": handle_bike_surfer_badge,
        "data_collector_level0": None,
        "data_collector_level1": None,
        "data_collector_level2": None,
        "data_collector_level3": None,
        "ecologist_level1": None,
        "ecologist_level2": None,
        "ecologist_level3": None,
        "healthy_level1": None,
        "healthy_level2": None,
        "healthy_level3": None,
        "public_mobility_level1": None,
        "public_mobility_level2": None,
        "public_mobility_level3": None,
        "money_saver_level1": None,
        "money_saver_level2": None,
        "money_saver_level3": None,
        "multi_surfer_level1": None,
        "multi_surfer_level2": None,
        "multi_surfer_level3": None,
        "new_user": None,
        "tpl_surfer_level1": None,
        "tpl_surfer_level2": None,
        "tpl_surfer_level3": None,
    }
    with db_connection:  # changes are committed when `with` block exits
        with db_connection.cursor() as cursor:
            track_info = get_track_info(track_id, cursor)
            badges_info = get_badges_info(track_info.owner_id, cursor)
            not_acquired = (b for b in badges_info if not b.acquired)
            for badge in not_acquired:
                logger.debug("handling badge {!r}...".format(badge.name))
                handler = badge_handlers.get(badge.name)
                handler(badge, track_info, cursor)


def handle_bike_surfer_badge(badge_info, track_info, db_cursor):
    if not uses_bike(track_info):
        return
    bike_distance = get_total_distance(
        track_info.owner_id, [VehicleType.bike], db_cursor)
    distance_km = bike_distance / 1000
    if distance_km >= badge_info.target:
        logger.debug("Awarding badge {!r}".format(badge_info.name))
        award_badge(badge_info.id, db_cursor)


def get_total_distance(user_id, vehicle_types: List[VehicleType], db_cursor):
    db_cursor.execute(
        get_query("select-total-distance-on-vehicle-types.sql"),
        {
            "user_id": user_id,
            "vehicle_types": [v.name for v in vehicle_types],
        }
    )
    return db_cursor.fetchone()


def handle_biker_badge(badge_info, track_info, db_cursor):
    if not uses_bike(track_info):
        return
    week_offset_days = {
        "biker_level1": 0,
        "biker_level2": 7,  # last week
        "biker_level3": 3 * 7,  # three weeks ago
    }.get(badge_info.name)
    now = dt.datetime.now(pytz.utc)
    end_bound = get_week_bounds(now)[-1]
    start_bound = get_week_bounds(
        now - dt.timedelta(days=week_offset_days)
    )[0]
    num_bike_usages = get_num_bike_usages(start_bound, end_bound)
    if num_bike_usages >= badge_info.target:
        logger.debug("Awarding badge {!r}".format(badge_info.name))
        award_badge(badge_info.id, db_cursor)


def award_badge(bage_id, db_cursor):
    db_cursor.execute(
        "update-badge-award.sql",
        {"badge_id": bage_id}
    )


def get_num_bike_usages(start_dt: dt.datetime, end_dt: dt.datetime, db_cursor):
    db_cursor.execute(
        get_query("select-count-bike-rides.sql"),
        {
            "start": start_dt.isoformat(),
            "end": end_dt.isoformat(),
        }
    )
    return len(db_cursor.fetchall())


def get_badges_info(user_id: int, db_cursor):
    db_cursor.execute(
        get_query("select-user-badges.sql"),
        {"user_id": user_id}
    )
    Badge = _get_record(db_cursor)
    result = []
    for row in db_cursor.fetchall():
        result.append(Badge(*row))
    return result


def get_track_info(track_id, db_cursor):
    db_cursor.execute(
        get_query("select-track.sql"),
        {"track_id": track_id}
    )
    TrackInfo = _get_record(db_cursor)
    return TrackInfo(*db_cursor.fetchone())


def uses_bike(track_info):
    return any(
        s["vehicle_type"] == VehicleType.bike.name for s in track_info.segments
    )


def _get_record(db_cursor):
    return namedtuple("Record", [i.name for i in db_cursor.description])
