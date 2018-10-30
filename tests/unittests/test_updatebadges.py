#########################################################################
#
# Copyright 2018, GeoSolutions Sas.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.
#
#########################################################################

import datetime as dt
from unittest import mock

import psycopg2
import pytest
import pytz

from smbbackend import updatebadges

pytestmark = pytest.mark.unit


DATE_FMT = "%Y-%m-%d"


@pytest.mark.parametrize(
    "badge_name, badge_target, track_created, existing, expected", [
        (
            "data_collector_level0",
            1,
            "2018-01-01",
            [
                "2018-01-01",
            ],
            1
        ),
        (
            "data_collector_level1",
            7,
            "2018-02-01",
            [
                "2018-02-01",
                "2018-01-31",
                "2018-01-30",
                "2018-01-29",
                "2018-01-28",
                "2018-01-27",
                "2018-01-26",
            ],
            7
        ),
        (
            "data_collector_level1",
            7,
            "2018-02-01",
            [
                "2018-02-01",
                "2018-01-31",
                "2018-01-30",
                "2018-01-29",
                "2018-01-27",
                "2018-01-26",
                "2018-01-25",
            ],
            0
        ),
        (
            "data_collector_level2",
            14,
            "2018-02-01",
            [
                "2018-02-01",
                "2018-01-31",
                "2018-01-30",
                "2018-01-29",
                "2018-01-28",
                "2018-01-27",
                "2018-01-26",
                "2018-01-25",
                "2018-01-24",
                "2018-01-23",
                "2018-01-22",
                "2018-01-21",
                "2018-01-20",
                "2018-01-19",
            ],
            14
        ),
        (
            "data_collector_level2",
            14,
            "2018-02-01",
            [
                "2018-02-01",
                "2018-01-31",
                "2018-01-30",
                "2018-01-29",
                "2018-01-28",
                "2018-01-27",
                "2018-01-26",
                "2018-01-25",
                "2018-01-24",
                "2018-01-23",
                "2018-01-22",
                "2018-01-21",
                "2017-01-20",
                "2018-01-19",
            ],
            0
        ),
        (
            "data_collector_level3",
            30,
            "2018-02-01",
            [
                "2018-02-01",
                "2018-01-31",
                "2018-01-30",
                "2018-01-29",
                "2018-01-28",
                "2018-01-27",
                "2018-01-26",
                "2018-01-25",
                "2018-01-24",
                "2018-01-23",
                "2018-01-22",
                "2018-01-21",
                "2018-01-20",
                "2018-01-19",
                "2018-01-18",
                "2018-01-17",
                "2018-01-16",
                "2018-01-15",
                "2018-01-14",
                "2018-01-13",
                "2018-01-12",
                "2018-01-11",
                "2018-01-10",
                "2018-01-09",
                "2018-01-08",
                "2018-01-07",
                "2018-01-06",
                "2018-01-05",
                "2018-01-04",
                "2018-01-03",
            ],
            30
        ),
        (
            "data_collector_level3",
            30,
            "2018-02-01",
            [
                "2018-02-01",
                "2018-01-31",
                "2018-01-30",
                "2018-01-29",
                "2018-01-28",
                "2018-01-27",
                "2018-01-26",
                "2018-01-25",
                "2018-01-24",
                "2018-01-23",
                "2018-01-22",
                "2018-01-21",
                "2018-01-20",
                "2018-01-19",
                "2018-01-18",
                "2018-01-17",
                "2018-01-16",
                "2018-01-15",
                "2018-01-14",
                "2018-01-13",
                "2018-01-12",
                "2018-01-11",
                "2018-01-10",
                "2018-01-09",
                "2018-01-08",
                "2018-01-07",
                "2018-01-06",
                "2018-01-05",
                "2017-01-04",
                "2018-01-03",
            ],
            0
        ),
    ], ids=[
        "level0_pass",
        "level1_pass",
        "level1_fail",
        "level2_pass",
        "level2_fail",
        "level3_pass",
        "level3_fail",
])
def test_handle_data_collector_badge(badge_name, badge_target, track_created,
                                     existing, expected):
    badge = updatebadges.BadgeInfo(
        id=1,
        name=badge_name,
        acquired=False,
        target=badge_target,
        progress=0
    )
    track = updatebadges.TrackInfo(
        id=1,
        created_at=dt.datetime.strptime(track_created, DATE_FMT),
        owner_id=1,
        aggregated_costs={},
        aggregated_emissions={},
        aggregated_health={},
        duration=1,
        start_date=None,
        end_date=None,
        length=1,
        segments=None,
        is_valid=True,
        validation_error=""
    )
    mock_rows = [(dt.datetime.strptime(i, DATE_FMT),) for i in existing]
    mock_cursor = mock.create_autospec(
        psycopg2.extensions.cursor, instance=True)
    mock_cursor.fetchall.return_value = mock_rows
    result = updatebadges.handle_data_collector_badge(
        badge, track, mock_cursor)
    assert result == expected


@pytest.mark.parametrize(
    "badge_name, track_created, existing, expected",
    [
        ("biker_level1", "2018-02-01", ["2018-02-01"], 1),
        ("biker_level1", "2018-02-01", ["2018-02-01", "2018-01-31"], 2),
    ]
)
def test_handle_biker_badge(badge_name, track_created, existing, expected):
    badge = updatebadges.BadgeInfo(
        id=1,
        name=badge_name,
        acquired=False,
        target=0,
        progress=0
    )
    track = updatebadges.TrackInfo(
        id=1,
        created_at=dt.datetime.strptime(track_created, DATE_FMT),
        owner_id=1,
        aggregated_costs={},
        aggregated_emissions={},
        aggregated_health={},
        duration=1,
        start_date=None,
        end_date=None,
        length=1,
        segments=[
            {
                "id": None,
                "vehicle_type": "bike",
                "length": 0,
            }
        ],
        is_valid=True,
        validation_error=""
    )
    mock_rows = [(dt.datetime.strptime(i, DATE_FMT),) for i in existing]
    mock_cursor = mock.create_autospec(
        psycopg2.extensions.cursor, instance=True)
    mock_cursor.fetchall.return_value = mock_rows
    result = updatebadges.handle_biker_badge(badge, track, mock_cursor)
    assert result == expected
