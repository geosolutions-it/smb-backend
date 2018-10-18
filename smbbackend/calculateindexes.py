#########################################################################
#
# Copyright 2018, GeoSolutions Sas.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.
#
#########################################################################

"""Calculation of smb indexes"""

from collections import namedtuple
import logging
import os
import pathlib

from . import _constants
from ._constants import VehicleType
from .utils import get_query
from .utils import get_track_info

logger = logging.getLogger(__name__)

_DATA_FIELDS = [
    "accelerationX",
    "accelerationY",
    "accelerationZ",
    "accuracy",
    "batConsumptionPerHour",
    "batteryLevel",
    "deviceBearing",
    "devicePitch",
    "deviceRoll",
    "elevation",
    "gps_bearing",
    "humidity",
    "latitude",
    "longitude",
    "lumen",
    "pressure",
    "proximity",
    "sessionId",
    "speed",
    "temperature",
    "timeStamp",
    "vehicleMode",
    "serialVersionUID",
]

PointData = namedtuple("PointData", _DATA_FIELDS)
SegmentInfo = namedtuple("SegmentInfo", [
    "id",
    "vehicle_type",
    "length_km",
    "duration_hours",
    "speed_km_h"
])


def calculate_indexes(track_id: str, db_connection):
    with db_connection:  # changes are committed when `with` block exits
        with db_connection.cursor() as cursor:
            track_info = get_track_info(track_id, cursor)
            if not track_info.is_valid:
                raise RuntimeError("Track {} is not valid".format(track_id))
            segments_info = get_segments_info(track_id, cursor)
            for index, info in enumerate(segments_info):
                emissions = calculate_emissions(
                    info.vehicle_type, info.length_km)
                costs = calculate_costs(
                    info.vehicle_type, info.length_km, info.duration_hours)
                duration_minutes = info.duration_hours * 60
                health = calculate_health(
                    info.vehicle_type, duration_minutes, info.speed_km_h)
                insert_segment_data(info.id, emissions, costs, health, cursor)
            update_track_aggregated_data(track_id, cursor)


def update_track_aggregated_data(track_id, db_cursor):
    query_kwargs = {"track_id": track_id}
    queries = [
        "update-track-aggregated-emissions.sql",
        "update-track-aggregated-costs.sql",
        "update-track-aggregated-health.sql",
        "update-track-info.sql",
    ]
    for query_file in queries:
        db_cursor.execute(get_query(query_file), query_kwargs)


def insert_segment_data(segment_id, emissions, costs, health, db_cursor):
    _perform_segment_insert(
        "insert-emission.sql", segment_id, emissions, db_cursor)
    _perform_segment_insert(
        "insert-cost.sql", segment_id, costs, db_cursor)
    _perform_segment_insert(
        "insert-health.sql", segment_id, health, db_cursor)


def get_segments_info(track_id, db_cursor):
    db_cursor.execute(
        get_query("get-segment-info.sql"),
        {"track_id": track_id}
    )
    result = []
    for row in db_cursor.fetchall():
        logger.debug("row: {}".format(row))
        segment_id, vehicle_type, length_meters, duration = row
        length_km = length_meters / 1000
        duration_hours = (
            duration.days * 24 +
            duration.seconds / (60 * 60) +
            duration.microseconds / (1000 * 1000 * 60 * 60)
        )
        logger.debug("duration_hours: {}".format(duration_hours))
        avg_speed = length_km / duration_hours  # km/h
        result.append(
            SegmentInfo(
                id=segment_id,
                vehicle_type=VehicleType[vehicle_type],
                length_km=length_km,
                duration_hours=duration_hours,
                speed_km_h=avg_speed
            )
        )
    return result


def _perform_segment_insert(query_filename, segment_id, query_params: dict,
                            db_cursor):
    all_query_params = query_params.copy()
    all_query_params["segment_id"] = segment_id
    db_cursor.execute(
        get_query(query_filename),
        all_query_params
    )


def calculate_emissions(vehicle_type: VehicleType,
                        segment_length:float) -> dict:
    result = {}
    for pollutant, coeffs in _constants.EMISSIONS.items():
        emitted = (
                (coeffs.get(vehicle_type, 0) * segment_length) /
                _constants.AVERAGE_PASSENGER_COUNT.get(vehicle_type, 1)
        )
        reference = (
                (coeffs[VehicleType.car] * segment_length) /
                _constants.AVERAGE_PASSENGER_COUNT[VehicleType.car]
        )
        saved = reference - emitted if vehicle_type != VehicleType.car else 0
        result.update({
            pollutant.name: emitted,
            "{}_saved".format(pollutant.name): saved
        })
    return result


def calculate_costs(vehicle_type: VehicleType, length_km: float,
                    duration_hours: float):
    public_transports = [
        vehicle_type.bus,
        vehicle_type.train,
    ]
    if vehicle_type in public_transports:
        costs = _calculate_costs_public_transportation(
            vehicle_type, length_km, duration_hours)
    else:
        costs = _calculate_costs_private_vehicle(
            vehicle_type, length_km, duration_hours)
    result = {
        "fuel_cost": costs[0],
        "time_cost": costs[1],
        "depreciation_cost": costs[2],
        "operation_cost": costs[3],
        "total_cost": costs[4],
    }
    return result


def _calculate_costs_private_vehicle(vehicle_type: VehicleType,
                                     length_km: float, duration_hours: float):
    """Calculate monetary costs associated with using a private vehicle

    - Fuel, depreciation and operation costs do not take into account the
      passenger count as these costs are usually supported solely by the
      vehicle's owner, even if there are other passengers aboard
    - The total cost may be enlarged according to the vehicle type, in order
      to provide an account of other costs

    """

    fuel_cost = _get_fuel_cost(length_km, vehicle_type)
    time_cost = duration_hours * _constants.TIME_COST_PER_HOUR_EURO
    depreciation_cost = (
            length_km * _constants.DEPRECIATION_COST.get(vehicle_type, 0))
    operation_cost = length_km * _constants.OPERATION_COST.get(vehicle_type, 0)
    total_cost = (
            sum((fuel_cost, time_cost, depreciation_cost, operation_cost)) *
            (1 + _constants.TOTAL_COST_OVERHEAD.get(vehicle_type, 0))
    )

    return fuel_cost, time_cost, depreciation_cost, operation_cost, total_cost


def _calculate_costs_public_transportation(vehicle_type: VehicleType,
                                           length_km:float,
                                           duration_hours: float):
    fuel_cost = 0
    time_cost = duration_hours * _constants.TIME_COST_PER_HOUR_EURO
    depreciation_cost = 0
    operation_cost = 0
    total_cost = (
            sum((fuel_cost, time_cost, depreciation_cost, operation_cost)) *
            (1 + _constants.TOTAL_COST_OVERHEAD.get(vehicle_type, 0))
    )
    return fuel_cost, time_cost, depreciation_cost, operation_cost, total_cost


def _get_fuel_cost(length, vehicle_type):
    try:
        volume_spent = length * _constants.FUEL_CONSUMPTION[vehicle_type]
        monetary_cost = volume_spent * _constants.FUEL_PRICE[vehicle_type]
    except KeyError:
        monetary_cost = 0
    return monetary_cost


def calculate_health(vehicle_type, duration_minutes, speed_km_h):
    return {
        "calories_consumed": _get_consumed_calories(
            speed_km_h, duration_minutes, vehicle_type),
        # "benefit_index": None  # TODO
    }


def _get_consumed_calories(speed_km_h, duration_minutes, vehicle_type):
    try:
        steps = _constants.CALORY_CONSUMPTION[vehicle_type]["steps"]
    except KeyError:
        result = 0
    else:
        for step in steps:
            if speed_km_h < step["speed"]:
                consumption_per_minute = step["calories"]
                break
        else:
            consumption_per_minute = steps[-1]["calories"]
        result = consumption_per_minute * duration_minutes
    return result
