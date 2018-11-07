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
from functools import partial
from itertools import product
import datetime as dt
import json
import logging
from typing import List
from typing import Tuple

import pytz

from .utils import get_query
from ._constants import PrizeCriterium

logger = logging.getLogger(__name__)

LeaderBoardInfo = Tuple[PrizeCriterium, dict]

CompetitionInfo = namedtuple("CompetitionInfo", [
    "id",
    "name",
    "criteria",
    "winner_threshold",
    "start_date",
    "end_date",
    "age_groups",
])

CompetitorInfo = namedtuple("CompetitorInfo", [
    "points",
    "user_id",
    "age_range",
    "absolute_score"
])


def calculate_prizes(db_cursor) -> List[Tuple[CompetitionInfo, List[dict]]]:
    """Calculate results for currently open competitions"""
    now = dt.datetime.now(pytz.utc)
    open_competitions = get_open_competitions(db_cursor)
    expired = [c for c in open_competitions if c.end_date < now]
    logger.debug("number of open competitions: {}".format(
        len(open_competitions)))
    logger.debug("number of expired competitions: {}".format(
        len(expired)))
    result = []
    for competition in expired:
        logger.info(
            "Handling competition {}...".format(competition.id))
        leaderboard = get_leaderboard(competition, db_cursor)
        winners = select_competition_winners(competition, leaderboard)
        assign_competition_winners(winners, competition.id, db_cursor)
        close_competition(competition, leaderboard, db_cursor)
        result.append((competition, winners))
    return result


def close_competition(competition, leaderboard, db_cursor):
    """Save the closing leaderboard in the competition's DB entry"""
    db_cursor.execute(
        get_query("update-competition-leaderboard.sql"),
        {
            "leaderboard": json.dumps(leaderboard),
            "competition_id": json.dumps(competition.id),
        }
    )


def get_open_competitions(db_cursor) -> List[CompetitionInfo]:
    """Get information from the currently open competitions

    Open competitions are those whose start_date is lower than the current
    date and have no assigned winners yet.

    """

    now = dt.datetime.now(pytz.utc)
    db_cursor.execute(
        get_query("select-current-competitions-info.sql"),
        {"relevant_date": now}
    )
    return [CompetitionInfo(*row) for row in db_cursor.fetchall()]


def get_leaderboard(competition: CompetitionInfo, db_cursor) -> List[dict]:
    criteria_handlers = {
        PrizeCriterium.saved_so2: partial(get_emissions_ranking, "so2_saved"),
        PrizeCriterium.saved_co2: partial(get_emissions_ranking, "co2_saved"),
        PrizeCriterium.saved_co: partial(get_emissions_ranking, "co_saved"),
        PrizeCriterium.saved_nox: partial(get_emissions_ranking, "nox_saved"),
        PrizeCriterium.saved_pm10: partial(
            get_emissions_ranking, "pm10_saved"),
    }
    criteria_leaderboards = []
    threshold = len(competition.criteria) * competition.winner_threshold
    for criterium in competition.criteria:
        criterium_enumeration = PrizeCriterium(criterium)
        handler = criteria_handlers[criterium_enumeration]
        ranking = handler(
            competition.start_date,
            competition.end_date,
            competition.age_groups,
            threshold,
            db_cursor
        )
        criteria_leaderboards.append((criterium_enumeration, ranking))
    logger.debug("criteria_leaderboards: {}".format(criteria_leaderboards))
    return consolidate_leaderboards(criteria_leaderboards)


def get_user_score(competition: CompetitionInfo, user_id, db_cursor) -> dict:
    criteria_handlers = {
        PrizeCriterium.saved_so2: partial(get_emissions_score, "so2_saved"),
        PrizeCriterium.saved_co2: partial(get_emissions_score, "co2_saved"),
        PrizeCriterium.saved_co: partial(get_emissions_score, "co_saved"),
        PrizeCriterium.saved_nox: partial(get_emissions_score, "nox_saved"),
        PrizeCriterium.saved_pm10: partial(get_emissions_score, "pm10_saved"),
    }
    scores = {}
    for criterium in competition.criteria:
        criterium_enumeration = PrizeCriterium(criterium)
        handler = criteria_handlers[criterium_enumeration]
        score = handler(
            competition.start_date,
            competition.end_date,
            user_id,
            db_cursor
        )
        scores[criterium_enumeration] = score
    return scores


def select_competition_winners(competition: CompetitionInfo,
                               leaderboard: List[dict]) -> List[dict]:
    winner_threshold = competition.winner_threshold
    return leaderboard[:winner_threshold]


def assign_competition_winners(winners: List[dict], competition_id: int,
                               db_cursor):
    for index, winner in enumerate(winners):
        rank = index + 1
        logger.info("Assigning user {} as a winner (rank: {}) of competition "
                    "{}...".format(winner["user"], rank, competition_id))
        db_cursor.execute(
            get_query("insert-competition-winner.sql"),
            {
                "competition_id": competition_id,
                "user_id": winner["user"],
                "rank": rank,
            }
        )


def get_prize_names(competition_id: int, user_rank: int, db_cursor):
    db_cursor.execute(
        get_query("select-prize-name.sql"),
        {
            "competition_id": competition_id,
            "user_rank": user_rank
        }
    )
    return [record[0] for record in db_cursor.fetchall()]


def consolidate_leaderboards(
        leaderboards: List[LeaderBoardInfo]) -> List[dict]:
    participants = get_unique_participants([b[1] for b in leaderboards])
    null_competitor = CompetitorInfo(
        points=0, user_id=None, age_range=None, absolute_score=0)
    final_leaderboard = {}
    for participant, board_info in product(participants, leaderboards):
        criterium, board = board_info
        final_leaderboard.setdefault(participant, {"points": 0, "boards": {}})
        board_points = board.get(participant, null_competitor).points
        board_score = board.get(participant, null_competitor).absolute_score
        final_leaderboard[participant]["points"] += board_points
        final_leaderboard[participant]["boards"][criterium.name] = board_score
    score_divisor = sum(len(board) for board in leaderboards)
    total = []
    for user_id, data in final_leaderboard.items():
        total.append(
            {
                "user": user_id,
                "points": data["points"] / score_divisor,
                "criteria_points": data["boards"]
            }
        )
    return sorted(total, key=lambda i: i["points"])


def get_unique_participants(leaderboards: List[dict]) -> List[str]:
    all_participants = set()
    for board in leaderboards:
        for user_id in board.keys():
            all_participants.add(user_id)
    return list(all_participants)


def get_emissions_ranking(pollutant, start_date, end_date, age_groups,
                          winner_threshold, db_cursor) -> dict:
    query_text = get_query("select-pollutant-savings-leaderboard.sql")
    formatted_query = query_text.format(pollutant_name=pollutant)
    db_cursor.execute(
        formatted_query,
        {
            "start_date": start_date,
            "end_date": end_date,
            "age_groups": age_groups,
            "threshold": winner_threshold,
        }
    )
    leaderboard = {}
    for row in db_cursor.fetchall():
        info = CompetitorInfo(*row)
        leaderboard[info.user_id] = info
    return leaderboard


def get_emissions_score(pollutant, start_date, end_date, user_id, db_cursor):
    query_text = get_query("select-user-score-pollutant-savings.sql")
    formatted_query = query_text.format(pollutant_name=pollutant)
    db_cursor.execute(
        formatted_query,
        {
            "start_date": start_date,
            "end_date": end_date,
            "user_id": user_id,
        }
    )
    return db_cursor.fetchone()[0] or 0
