#########################################################################
#
# Copyright 2018, GeoSolutions Sas.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.
#
#########################################################################

"""Calculations for current competitions' scores

This module should be called periodically

"""

from collections import namedtuple
import datetime as dt
from typing import List

import pytz

from .utils import get_query
from ._constants import AgeRange


CompetitionInfo = namedtuple("CompetitionInfo", [
    "id",
    "name",
    "criteria",
    "repeat_when",
    "winner_threshold",
    "start_date",
    "end_date",
    "age_group",
])

CompetitorInfo = namedtuple("CompetitorInfo", [
    "id",
    "age_range",
    "score"
])


def calculate_prizes(db_connection):
    with db_connection:  # changes are committed when `with` block exits
        with db_connection.cursor() as cursor:
            to_evaluate = get_open_competitions(cursor)
            for competition in to_evaluate:
                leaderboard = get_leaderboard(competition)
                for winner in leaderboard:
                    assign_competition_winner(winner, competition.id)


def get_open_competitions(db_cursor):
    now = dt.datetime.now(pytz.utc)
    yesterday = now - dt.timedelta(days=1)
    db_cursor.execute(
        get_query("select-current-competitions-info.sql"),
        {"relevant_date": yesterday}
    )
    return [CompetitionInfo(*row) for row in db_cursor.fetchall()]


def get_leaderboard(competition_id):
    return []


def assign_competition_winner(user_id, competition_id):
    pass


def get_co2_savings_leaderboard(start_date: dt.datetime, end_date: dt.datetime,
                                winner_threshold: int, db_cursor):
    query_text = get_query("select-pollutant-savings-leaderboard.sql")
    formatted_query = query_text.format(pollutant_name="co2_saved")
    db_cursor.execute(
        formatted_query,
        {
            "start_date": start_date,
            "end_date": end_date,
            "threshold": winner_threshold * (len(AgeRange) - 1),
        }
    )
    result = {}
    for row in db_cursor.fetchall():
        info = CompetitorInfo(*row)
        age_enumeration = AgeRange(info.age_range)
        entries = result.setdefault(age_enumeration, [])
        entries.append(info)
    return result
