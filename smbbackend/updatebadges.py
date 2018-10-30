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

from .utils import get_query
from .utils import get_week_bounds
from .utils import get_track_info
from .utils import TrackInfo
from ._constants import BadgeName
from ._constants import PUBLIC_TRANSPORTS
from ._constants import SUSTAINABLE_TRANSPORTS
from ._constants import VehicleType

logger = logging.getLogger(__name__)

BadgeInfo = namedtuple("BadgeInfo", [
    "id",
    "name",
    "acquired",
    "target",
    "progress",
])


UNHANDLED_BADGES = [
    BadgeName.new_user,
    BadgeName.money_saver_level1,  # not implemented yet
    BadgeName.money_saver_level2,  # not implemented yet
    BadgeName.money_saver_level3,  # not implemented yet
]


def update_badges(track_id: int, db_cursor) -> List[BadgeName]:
    """Update badges taking into account for the input track

    Note that this function does not check for track validity. The caller is
    responsible for that (if needed)

    """

    track_info = get_track_info(track_id, db_cursor)
    badges_info = get_badges_info(track_info.owner_id, db_cursor)
    not_acquired = (b for b in badges_info if not b.acquired)
    to_ignore = (b for b in badges_info if b.name in UNHANDLED_BADGES)
    to_handle = set(not_acquired).difference(set(to_ignore))
    awarded_badges = []
    for badge_info in list(to_handle):
        logger.debug("handling badge {!r}...".format(badge_info.name))
        badge_awarded = handle_badge(badge_info, track_info, db_cursor)
        if badge_awarded:
            awarded_badges.append(badge_info.name)
    return awarded_badges


def handle_badge(badge: BadgeInfo, track: TrackInfo, db_cursor):
    handler = {
        BadgeName.biker_level1: handle_biker_badge,
        BadgeName.biker_level2: handle_biker_badge,
        BadgeName.biker_level3: handle_biker_badge,
        BadgeName.bike_surfer_level1: handle_distance_based_badge,
        BadgeName.bike_surfer_level2: handle_distance_based_badge,
        BadgeName.bike_surfer_level3: handle_distance_based_badge,
        BadgeName.data_collector_level0: handle_data_collector_badge,
        BadgeName.data_collector_level1: handle_data_collector_badge,
        BadgeName.data_collector_level2: handle_data_collector_badge,
        BadgeName.data_collector_level3: handle_data_collector_badge,
        BadgeName.ecologist_level1: handle_ecologist_badge,
        BadgeName.ecologist_level2: handle_ecologist_badge,
        BadgeName.ecologist_level3: handle_ecologist_badge,
        BadgeName.healthy_level1: handle_healthy_badge,
        BadgeName.healthy_level2: handle_healthy_badge,
        BadgeName.healthy_level3: handle_healthy_badge,
        BadgeName.public_mobility_level1: handle_public_mobility_badge,
        BadgeName.public_mobility_level2: handle_public_mobility_badge,
        BadgeName.public_mobility_level3: handle_public_mobility_badge,
        BadgeName.money_saver_level1: None,
        BadgeName.money_saver_level2: None,
        BadgeName.money_saver_level3: None,
        BadgeName.multi_surfer_level1: handle_distance_based_badge,
        BadgeName.multi_surfer_level2: handle_distance_based_badge,
        BadgeName.multi_surfer_level3: handle_distance_based_badge,
        BadgeName.tpl_surfer_level1: handle_distance_based_badge,
        BadgeName.tpl_surfer_level2: handle_distance_based_badge,
        BadgeName.tpl_surfer_level3: handle_distance_based_badge,
    }[badge.name]
    current_progress = handler(badge, track, db_cursor)
    badge_awarded = False
    if current_progress >= badge.target:
        logger.debug("Awarding badge {!r}".format(badge.name))
        award_badge(badge.id, db_cursor)
        badge_awarded = True
    return badge_awarded


def handle_data_collector_badge(badge: BadgeInfo, track: TrackInfo, db_cursor):
    days_offset = {
        BadgeName.data_collector_level0: 0,
        BadgeName.data_collector_level1: 7 - 1,
        BadgeName.data_collector_level2: 14 - 1,
        BadgeName.data_collector_level3: 30 - 1,
    }
    end_date = track.created_at
    start_date = end_date - dt.timedelta(days=days_offset.get(badge.name, 0))
    delta_days = (end_date - start_date).days
    interval = [start_date + dt.timedelta(days=d) for d in range(delta_days)]
    interval = interval or [end_date]
    db_cursor.execute(
        get_query("select-count-tracks-by-date-interval.sql"),
        {
            "start_date": start_date,
            "end_date": end_date,
        }
    )
    rows = db_cursor.fetchall()
    comparison_pattern = "%Y-%m-%d"
    days_with_data_collected = [
        r[0].strftime(comparison_pattern) for r in rows]
    for day in interval:
        if day.strftime(comparison_pattern) not in days_with_data_collected:
            result = 0
            break
    else:
        result = badge.target  # all days in interval had some data collected
    return result


def handle_biker_badge(badge: BadgeInfo, track: TrackInfo,
                       db_cursor):
    if uses_vehicle_type(track.segments, VehicleType.bike):
        week_offset_days = {
            BadgeName.biker_level1: 0,
            BadgeName.biker_level2: 7,  # previous week
            BadgeName.biker_level3: 3 * 7,  # three weeks ago
        }.get(badge.name)
        reference_date = track.created_at
        end_bound = get_week_bounds(reference_date)[-1]
        start_bound = get_week_bounds(
            reference_date - dt.timedelta(days=week_offset_days)
        )[0]
        num_bike_usages = get_num_bike_usages(start_bound, end_bound, db_cursor)
    else:
        num_bike_usages = 0
    return num_bike_usages


def handle_distance_based_badge(badge: BadgeInfo, track: TrackInfo, db_cursor):
    vehicle_type_mapping = {
        "bike_surfer": [VehicleType.bike],
        "tpl_surfer": [VehicleType.bus],
        "multi_surfer": SUSTAINABLE_TRANSPORTS,
    }
    for badge_name_prefix, vehicle_types in vehicle_type_mapping.items():
        if badge.name.name.startswith(badge_name_prefix):
            total_distance = get_total_distance(
                track.owner_id, vehicle_types, db_cursor)
            break
    else:
        total_distance = 0
    distance_km = total_distance / 1000 if total_distance is not None else 0
    return distance_km


def handle_ecologist_badge(badge: BadgeInfo, track: TrackInfo, db_cursor):
    db_cursor.execute(
        get_query("select-pollutant-sum-by-user.sql"),
        {
            "pollutant": "co2_saved",
            "user_id": track.owner_id
        }
    )
    saved_co2 = db_cursor.fetchone()[0]
    saved_co2_kg = (saved_co2 or 0) / 1000
    return saved_co2_kg


def handle_public_mobility_badge(badge: BadgeInfo, track: TrackInfo,
                                 db_cursor):
    db_cursor.execute(
        get_query("select-count-vehicle-rides.sql"),
        {
            "vehicle_types": [t.name for t in PUBLIC_TRANSPORTS],
            "user_id": track.owner_id,
        }
    )
    num_rides = db_cursor.fetchone()[0]
    return num_rides


def handle_healthy_badge(badge: BadgeInfo, track: TrackInfo, db_cursor):
    db_cursor.execute(
        get_query("select-sum-consumed-calories.sql"),
        {"user_id": track.owner_id}
    )
    consumed_calories = db_cursor.fetchone()[0]
    return consumed_calories


def get_total_distance(user_id, vehicle_types: List[VehicleType], db_cursor):
    db_cursor.execute(
        get_query("select-total-distance-on-vehicle-types.sql"),
        {
            "user_id": user_id,
            "vehicle_types": [v.name for v in vehicle_types],
        }
    )
    return db_cursor.fetchone()[0]


def award_badge(bage_id, db_cursor):
    db_cursor.execute(
        get_query("update-badge-award.sql"),
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


def get_badges_info(user_id: int, db_cursor) -> List[BadgeInfo]:
    db_cursor.execute(
        get_query("select-user-badges.sql"),
        {"user_id": user_id}
    )
    result = []
    for row in db_cursor.fetchall():
        result.append(
            BadgeInfo(
                id=row[0],
                name=BadgeName(row[1]),
                acquired=row[2],
                target=row[3],
                progress=row[4],
            )
        )
    return result


def uses_vehicle_type(segments_info: List[dict], *vehicle_types: VehicleType):
    vehicle_type_names = [vt.name for vt in vehicle_types]
    return any(s["vehicle_type"] in vehicle_type_names for s in segments_info)
