#########################################################################
#
# Copyright 2019, GeoSolutions Sas.
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
import datetime as dt
import json
import logging
import typing

import pytz

from .utils import get_query
from ._constants import PrizeCriterium

logger = logging.getLogger(__name__)


CompetitionInfo = namedtuple("CompetitionInfo", [
    "id",
    "name",
    "criteria",
    "winner_threshold",
    "start_date",
    "end_date",
    "age_groups",
    "region_of_interest"
])

CompetitorInfo = namedtuple("CompetitorInfo", [
    "points",
    "user_id",
    "absolute_score"
])

LeaderBoardInfo = typing.Tuple[PrizeCriterium, typing.List[CompetitorInfo]]


def calculate_prizes(
        db_cursor
) -> typing.List[typing.Tuple[CompetitionInfo, typing.List[dict]]]:
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


def get_open_competitions(db_cursor) -> typing.List[CompetitionInfo]:
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


def get_leaderboard(
        competition: CompetitionInfo,
        db_cursor
) -> typing.List[dict]:
    criteria_handlers = {
        PrizeCriterium.saved_so2: partial(get_emissions_ranking, "so2_saved"),
        PrizeCriterium.saved_co2: partial(get_emissions_ranking, "co2_saved"),
        PrizeCriterium.saved_co: partial(get_emissions_ranking, "co_saved"),
        PrizeCriterium.saved_nox: partial(get_emissions_ranking, "nox_saved"),
        PrizeCriterium.saved_pm10: partial(
            get_emissions_ranking, "pm10_saved"),
    }
    criteria_leaderboards = {}
    threshold = len(competition.criteria) * competition.winner_threshold
    for criterium in competition.criteria:
        criterium_enumeration = PrizeCriterium(criterium)
        handler = criteria_handlers[criterium_enumeration]
        criteria_leaderboards[criterium_enumeration.value] = handler(
            competition,
            threshold,
            db_cursor
        )
    logger.debug("criteria_leaderboards: {}".format(criteria_leaderboards))
    return consolidate_leaderboards(criteria_leaderboards)


def consolidate_leaderboards(
        leaderboards: typing.Dict[PrizeCriterium, typing.List[CompetitorInfo]]
) -> typing.List[dict]:
    """Return a sorted list with consolidated leaderboards

    A user's leaderboards are consolidated into a single one by averaging the
    points gotten in each leaderboard.

    The final leaderboard is sorted in reverse order according to the final
    score of each user. This means that the winner shall be the first in the
    list.

    """

    summed_participant_scores = sum_participant_scores(leaderboards)
    score_divisor = len(leaderboards)
    final_leaderboard = []
    for user_id, user_scores in summed_participant_scores.items():
        final_leaderboard.append(
            {
                "user": user_id,
                "points": user_scores["points"] / score_divisor,
                "criteria_points": user_scores["boards"],
            }
        )
    return sorted(final_leaderboard, key=lambda i: i["points"], reverse=True)


def sum_participant_scores(
        leaderboards: typing.Dict[
            PrizeCriterium,
            typing.List[CompetitorInfo]
        ]
) -> typing.Dict:
    """Sum participant's scores across all criteria of a competition

    A competition may use several criteria in order to attribute winners.
    This function takes the leaderboards for each criterium and returns
    an aggregated result per user.

    """

    consolidated_scores = {}
    for criterium, leaderboard in leaderboards.items():
        for competitor_info in leaderboard:
            user_scores = consolidated_scores.setdefault(
                competitor_info.user_id,
                {
                    "points": 0,
                    "absolute_score": 0,
                    "boards": {}
                }
            )
            user_scores["points"] += competitor_info.points
            user_scores["absolute_score"] += competitor_info.absolute_score
            user_scores["boards"][criterium] = competitor_info.absolute_score
    return consolidated_scores


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
            competition,
            user_id,
            db_cursor
        )
        scores[criterium_enumeration] = score
    return scores


def select_competition_winners(
        competition: CompetitionInfo,
        leaderboard: typing.List[dict]
) -> typing.List[dict]:
    winner_threshold = competition.winner_threshold
    return leaderboard[:winner_threshold]


def assign_competition_winners(
        winners: typing.List[dict],
        competition_id: int,
        db_cursor
):
    for index, winner in enumerate(winners):
        rank = index + 1
        logger.info("Assigning user {} as a winner (rank: {}) of competition "
                    "{}...".format(winner["user"], rank, competition_id))
        db_cursor.execute(
            get_query("select-competitionparticipant.sql"),
            {
                "competition_id": competition_id,
                "user_id": winner["user"]
            }
        )
        participant_record = db_cursor.fetchone()
        participant_id = participant_record[0]
        db_cursor.execute(
            get_query("insert-competition-winner.sql"),
            {
                "participant_id": participant_id,
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


def get_emissions_ranking(
        pollutant: str,
        competition: CompetitionInfo,
        winner_threshold: int,
        db_cursor
) -> typing.List[CompetitorInfo]:
    if competition.region_of_interest is not None:
        query_path = "select-pollutant-savings-leaderboard-with-roi.sql"
    else:
        query_path = "select-pollutant-savings-leaderboard.sql"
    query_text = get_query(query_path)
    formatted_query = query_text.format(pollutant_name=pollutant)
    db_cursor.execute(
        formatted_query,
        {
            "competition_id": competition.id,
            "threshold": winner_threshold,
        }
    )
    leaderboard = []
    for row in db_cursor.fetchall():
        leaderboard.append(CompetitorInfo(*row))
    else:
        if len(leaderboard) == 0:
            logger.debug("Leaderboard is empty")
    return leaderboard


def get_emissions_score(
        pollutant: str,
        competition: CompetitionInfo,
        user_id: int,
        db_cursor
):
    if competition.region_of_interest is not None:
        query_path = "select-user-score-pollutant-savings-with-roi.sql"
    else:
        query_path = "select-user-score-pollutant-savings.sql"
    query_text = get_query(query_path)
    formatted_query = query_text.format(pollutant_name=pollutant)
    db_cursor.execute(
        formatted_query,
        {
            "competition_id": competition.id,
            "user_id": user_id,
        }
    )
    return db_cursor.fetchone()[0] or 0
