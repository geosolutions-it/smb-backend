#########################################################################
#
# Copyright 2018, GeoSolutions Sas.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.
#
#########################################################################

"""Constants used for calculating segment data"""

from enum import Enum


class VehicleType(Enum):
    foot = 1
    bike = 2
    bus = 3
    car = 4
    scooter = 5
    motorbike = 6
    average_motorbike = 7
    train = 8
    unknown = 9


class Pollutant(Enum):
    so2 = 1
    nox = 2
    co = 3
    co2 = 4
    pm10 = 5


AVERAGE_PASSENGER_COUNT = {
    VehicleType.foot: 1,
    VehicleType.bike: 1,
    VehicleType.scooter: 1,
    VehicleType.motorbike: 1,
    VehicleType.average_motorbike: 1,
    VehicleType.bus: 40,
    VehicleType.car: 1.5,
    VehicleType.train: 50,
}


def _get_average(*args):
    return sum(args) / len(args)


EMISSIONS = {
    Pollutant.so2: {
        "unit": "mg/km",
        VehicleType.car: 1.1,
        VehicleType.bus: 4.4,
        VehicleType.scooter: 0.3,
        VehicleType.motorbike: 0.6,
        VehicleType.train: 0,
    },
    Pollutant.nox: {
        "unit": "mg/km",
        VehicleType.car: 460,
        VehicleType.bus: 6441,
        VehicleType.scooter: 158,
        VehicleType.motorbike: 165,
        VehicleType.train: 0,
    },
    Pollutant.co: {
        "unit": "mg/km",
        VehicleType.car: 617,
        VehicleType.bus: 1451,
        VehicleType.scooter: 5282,
        VehicleType.motorbike: 6505,
        VehicleType.train: 0,
    },
    Pollutant.co2: {
        "unit": "g/km",
        VehicleType.car: 177,
        VehicleType.bus: 668,
        VehicleType.scooter: 49,
        VehicleType.motorbike: 100,
        VehicleType.train: 65,
    },
    Pollutant.pm10: {
        "unit": "mg/km",
        VehicleType.car: 46,
        VehicleType.bus: 273,
        VehicleType.scooter: 96,
        VehicleType.motorbike: 34,
        VehicleType.train: 0,
    },
}
EMISSIONS[Pollutant.so2][VehicleType.average_motorbike] = _get_average(
    EMISSIONS[Pollutant.so2][VehicleType.scooter],
    EMISSIONS[Pollutant.so2][VehicleType.motorbike]
)
EMISSIONS[Pollutant.nox][VehicleType.average_motorbike] = _get_average(
    EMISSIONS[Pollutant.nox][VehicleType.scooter],
    EMISSIONS[Pollutant.nox][VehicleType.motorbike]
)
EMISSIONS[Pollutant.co][VehicleType.average_motorbike] = _get_average(
    EMISSIONS[Pollutant.co][VehicleType.scooter],
    EMISSIONS[Pollutant.co][VehicleType.motorbike]
)
EMISSIONS[Pollutant.co2][VehicleType.average_motorbike] = _get_average(
    EMISSIONS[Pollutant.co2][VehicleType.scooter],
    EMISSIONS[Pollutant.co2][VehicleType.motorbike]
)
EMISSIONS[Pollutant.pm10][VehicleType.average_motorbike] = _get_average(
    EMISSIONS[Pollutant.pm10][VehicleType.scooter],
    EMISSIONS[Pollutant.pm10][VehicleType.motorbike]
)

FUEL_PRICE = {
    "unit": "eur/l",
    VehicleType.bus: 1.432,
    VehicleType.car: 1.417,
    VehicleType.motorbike: 1.545,
    VehicleType.scooter: 1.545,
    VehicleType.train: 0,
}
FUEL_PRICE[VehicleType.average_motorbike] = _get_average(
    FUEL_PRICE[VehicleType.scooter], FUEL_PRICE[VehicleType.motorbike])

FUEL_CONSUMPTION = {
    "unit": "km/l",
    VehicleType.bus: 3,
    VehicleType.car: 11.5,
    VehicleType.motorbike: 20,
    VehicleType.scooter: 30,
    VehicleType.train: 0,
}
FUEL_CONSUMPTION[VehicleType.average_motorbike] = _get_average(
    FUEL_CONSUMPTION[VehicleType.scooter],
    FUEL_CONSUMPTION[VehicleType.motorbike]
)

TIME_COST_PER_HOUR_EURO = 8

DEPRECIATION_COST = {
    "unit": "euro/km",
    VehicleType.bus: 0,
    VehicleType.car: 0.106,
    VehicleType.motorbike: 0.111,
    VehicleType.scooter: 0.089,
    VehicleType.train: 0,
}
DEPRECIATION_COST[VehicleType.average_motorbike] = _get_average(
    DEPRECIATION_COST[VehicleType.scooter],
    DEPRECIATION_COST[VehicleType.motorbike]
)

OPERATION_COST = {
    "unit": "euro/km",
    VehicleType.bus: 0,
    VehicleType.car: 0.072,
    VehicleType.motorbike: 0.058,
    VehicleType.scooter: 0.162,
    VehicleType.train: 0,
}
OPERATION_COST[VehicleType.average_motorbike] = _get_average(
    OPERATION_COST[VehicleType.scooter],
    OPERATION_COST[VehicleType.motorbike]
)

TOTAL_COST_OVERHEAD = {
    VehicleType.car: 0.2,
}

CALORY_CONSUMPTION = {
    "unit": "cal/minute",
    VehicleType.foot: {
        "unit": "km/h",
        "steps": [
            {"speed": 5.5, "calories": 5.28},
            {"speed": 6.5, "calories": 5.94},
        ],
    },
    VehicleType.bike: {
        "unit": "km/h",
        "steps": [
            {"speed": 13, "calories": 4.87},
            {"speed": 19, "calories": 7.03},
            {"speed": 24, "calories": 9.26},
            {"speed": 27, "calories": 11.14},
            {"speed": 30, "calories": 13.38},
        ],
    },
}

HEALTH_BENEFIT_INDEX = {
    VehicleType.foot: {
        "applicable_age": (20, 74),
        "relatve_risk": 0.883,
        "threshold": (146, "hours/year"),
        "maximum_percentage": 0.3
    },
    VehicleType.bike: {
        "applicable_age": (20, 64),
        "relatve_risk": 0.899,
        "threshold": (87, "hours/year"),
        "maximum_percentage": 0.45
    },
}
