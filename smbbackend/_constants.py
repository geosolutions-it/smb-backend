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
    motorcycle = 5
    train = 6
    unknown = 7


class Pollutant(Enum):
    so2 = 1
    nox = 2
    co = 3
    co2 = 4
    pm10 = 5


AVERAGE_PASSENGER_COUNT = {
    VehicleType.foot: 1,
    VehicleType.bike: 1,
    VehicleType.motorcycle: 1,
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
        VehicleType.motorcycle: (0.3 + 0.6) / 2,  # avg scooter and motorbike
        VehicleType.train: 0,
    },
    Pollutant.nox: {
        "unit": "mg/km",
        VehicleType.car: 460,
        VehicleType.bus: 6441,
        VehicleType.motorcycle: (158 + 165) / 2,  # avg scooter and motorbike
        VehicleType.train: 0,
    },
    Pollutant.co: {
        "unit": "mg/km",
        VehicleType.car: 617,
        VehicleType.bus: 1451,
        VehicleType.motorcycle: (5282 + 6505) / 2,  # avg scooter and motorbike
        VehicleType.train: 0,
    },
    Pollutant.co2: {
        "unit": "g/km",
        VehicleType.car: 177,
        VehicleType.bus: 668,
        VehicleType.motorcycle: (49 + 100) / 2,  # avg scooter and motorbike
        VehicleType.train: 65,
    },
    Pollutant.pm10: {
        "unit": "mg/km",
        VehicleType.car: 46,
        VehicleType.bus: 273,
        VehicleType.motorcycle: (96 + 34) / 2,  # avg scooter and motorbike
        VehicleType.train: 0,
    },
}

FUEL_PRICE = {
    "unit": "eur/l",
    VehicleType.bus: 1.432,
    VehicleType.car: 1.417,
    VehicleType.motorcycle: 1.545,
    VehicleType.train: 0,
}

FUEL_CONSUMPTION = {
    "unit": "km/l",
    VehicleType.bus: 3,
    VehicleType.car: 11.5,
    VehicleType.motorcycle: (20 + 30) / 2,  # avg scooter and motorbike
    VehicleType.train: 0,
}

TIME_COST_PER_HOUR_EURO = 8

DEPRECIATION_COST = {
    "unit": "euro/km",
    VehicleType.bus: 0,
    VehicleType.car: 0.106,
    VehicleType.motorcycle: (0.111 + 0.089) / 2,  # avg scooter and motorbike
    VehicleType.train: 0,
}

OPERATION_COST = {
    "unit": "euro/km",
    VehicleType.bus: 0,
    VehicleType.car: 0.072,
    VehicleType.motorcycle: (0.058 + 0.162) / 2,  # avg scooter and motorbike
    VehicleType.train: 0,
}

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
        "relative_risk": 0.883,
        "threshold": (146, "hours/year"),
        "maximum_percentage": 0.3
    },
    VehicleType.bike: {
        "applicable_age": (20, 64),
        "relative_risk": 0.899,
        "threshold": (87, "hours/year"),
        "maximum_percentage": 0.45
    },
}
