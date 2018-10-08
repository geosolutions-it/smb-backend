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


class PrizeCriterium(Enum):
    saved_so2 = "saved SO2 emissions"
    saved_nox = "saved NOx emissions"
    saved_co2 = "saved CO2 emissions"
    saved_co = "saved CO emissions"
    saved_pm10 = "saved PM10 emissions"
    consumed_calories = "consumed calories"
    bike_usage_frequency = "bike usage frequency"
    public_transport_usage_frequency = "public transport usage frequency"
    bike_distance = "bike distance"
    sustainable_means_distance = "sustainable means distance"


class AgeRange(Enum):
    nineteen_or_younger = "< 19"
    between_nineteen_and_thirty = "19 - 30"
    between_thirty_and_sixty_five = "30- 65"
    older_than_sixty_five = "65+"


class BadgeName(Enum):
    new_user = "01_new_user"
    data_collector_level0 = "02_data_collector_level0"
    data_collector_level1 = "03_data_collector_level1"
    data_collector_level2 = "04_data_collector_level2"
    data_collector_level3 = "05_data_collector_level3"
    biker_level1 = "06_biker_level1"
    biker_level2 = "07_biker_level2"
    biker_level3 = "08_biker_level3"
    public_mobility_level1 = "09_public_mobility_level1"
    public_mobility_level2 = "10_public_mobility_level2"
    public_mobility_level3 = "11_public_mobility_level3"
    bike_surfer_level1 = "12_bike_surfer_level1"
    bike_surfer_level2 = "13_bike_surfer_level2"
    bike_surfer_level3 = "14_bike_surfer_level3"
    tpl_surfer_level1 = "15_tpl_surfer_level1"
    tpl_surfer_level2 = "16_tpl_surfer_level2"
    tpl_surfer_level3 = "17_tpl_surfer_level3"
    multi_surfer_level1 = "18_multi_surfer_level1"
    multi_surfer_level2 = "19_multi_surfer_level2"
    multi_surfer_level3 = "20_multi_surfer_level3"
    ecologist_level1 = "21_ecologist_level1"
    ecologist_level2 = "22_ecologist_level2"
    ecologist_level3 = "23_ecologist_level3"
    healthy_level1 = "24_healthy_level1"
    healthy_level2 = "25_healthy_level2"
    healthy_level3 = "26_healthy_level3"
    money_saver_level1 = "27_money_saver_level1"
    money_saver_level2 = "28_money_saver_level2"
    money_saver_level3 = "29_money_saver_level3"


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

SUSTAINABLE_TRANSPORTS = [
    VehicleType.bike,
    VehicleType.bus,
    VehicleType.foot,
    VehicleType.train,
]

PUBLIC_TRANSPORTS = [
    VehicleType.bus,
    VehicleType.train,
]


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
