#########################################################################
#
# Copyright 2019, GeoSolutions Sas.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.
#
#########################################################################

import pytest

from smbbackend import calculateprizes
from smbbackend._constants import PrizeCriterium

pytestmark = pytest.mark.unit


@pytest.mark.parametrize(
    "summed_participant_scores", [
        pytest.param(
            {
                "fake_user1": {},
            },
        )
    ]
)
def test_consolidate_leaderboards(summed_participant_scores):
    result = calculateprizes.consolidate_leaderboards(criteria_boards)
    expected = None
    assert result == expected


@pytest.mark.parametrize(
    "criteria_boards, expected",
    [
        pytest.param(
            {
                PrizeCriterium.saved_co2: [
                    calculateprizes.CompetitorInfo(
                        user_id="fake_user1",
                        points=1,
                        absolute_score=20
                    ),
                    calculateprizes.CompetitorInfo(
                        user_id="fake_user2",
                        points=0,
                        absolute_score=0
                    ),
                ]
            },
            {
                "fake_user1": {
                    "points": 1,
                    "absolute_score": 20,
                    "boards": {
                        PrizeCriterium.saved_co2: 20
                    },
                },
                "fake_user2": {
                    "points": 0,
                    "absolute_score": 0,
                    "boards": {
                        PrizeCriterium.saved_co2: 0
                    },
                },
            },
            id="single_criterium"
        ),
        pytest.param(
            {
                PrizeCriterium.saved_co2: [
                    calculateprizes.CompetitorInfo(
                        user_id="fake_user1",
                        points=1,
                        absolute_score=20
                    ),
                    calculateprizes.CompetitorInfo(
                        user_id="fake_user2",
                        points=0,
                        absolute_score=0
                    ),
                ],
                PrizeCriterium.saved_so2: [
                    calculateprizes.CompetitorInfo(
                        user_id="fake_user1",
                        points=3,
                        absolute_score=30
                    ),
                    calculateprizes.CompetitorInfo(
                        user_id="fake_user3",
                        points=2,
                        absolute_score=15
                    ),
                ]
            },
            {
                "fake_user1": {
                    "points": 4,
                    "absolute_score": 50,
                    "boards": {
                        PrizeCriterium.saved_co2: 20,
                        PrizeCriterium.saved_so2: 30,
                    },
                },
                "fake_user2": {
                    "points": 0,
                    "absolute_score": 0,
                    "boards": {
                        PrizeCriterium.saved_co2: 0,
                    },
                },
                "fake_user3": {
                    "points": 2,
                    "absolute_score": 15,
                    "boards": {
                        PrizeCriterium.saved_so2: 15
                    },
                },
            },
            id="two_criteria"
        ),
    ]
)
def test_sum_participant_scores(criteria_boards, expected):
    result = calculateprizes.sum_participant_scores(criteria_boards)
    assert result == expected
