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

import pytest
import pytz

pytestmark = pytest.mark.unit

from smbbackend import processor
from smbbackend._constants import VehicleType
from smbbackend import exceptions


@pytest.mark.parametrize("raw_data, expected", [
    (
        (
            "\n0,0,0,0,0,0,0,0,0,0,0,0,43.8400862704083,10.5083749143491,0,0,"
            "0,1537193729,15,0,1536830986000,2,0"
        ),
        [
            processor.PointData(
                latitude=43.8400862704083,
                longitude=10.5083749143491,
                accuracy=0.0,
                speed=15.0,
                timestamp=dt.datetime(2018, 9, 13, 9, 29, 46, tzinfo=pytz.utc),
                vehicle_type=VehicleType.bus,
                session_id=1537193729,
                acceleration_x=0,
                acceleration_y=0,
                acceleration_z=0,
                battery_consumption_per_hour=0,
                battery_level=0,
                device_bearing=0,
                device_pitch=0,
                device_roll=0,
                elevation=0,
                gps_bearing=0,
                humidity=0,
                lumen=0,
                pressure=0,
                proximity=0,
                serial_version_uid="dummy",
                temperature=0,
            )
        ]
    ),
    pytest.param("", [], id="no data"),
    pytest.param("dummy", [], id="only header"),
    pytest.param(
        (
            "\n"
            "0,0,0,0,0,0,0,0,0,0,0,0,43.8401082380725,10.5084723757338,0,0,"
            "0,1537193729,15,0,1536830988728,2,0\n"
            "0,0,0,0,0,0,0,0,0,0,0,0,43.8401521733766,10.5085820197916,0,0,0,"
            "1537193729,15,0,1536830992079,2,0"
        ),
        [
            processor.PointData(
                latitude=43.8401082380725,
                longitude=10.5084723757338,
                accuracy=0.0,
                speed=15.0,
                timestamp=dt.datetime(
                    2018, 9, 13, 9, 29, 48, 728000, tzinfo=pytz.utc),
                vehicle_type=VehicleType.bus,
                session_id=1537193729,
                acceleration_x=0,
                acceleration_y=0,
                acceleration_z=0,
                battery_consumption_per_hour=0,
                battery_level=0,
                device_bearing=0,
                device_pitch=0,
                device_roll=0,
                elevation=0,
                gps_bearing=0,
                humidity=0,
                lumen=0,
                pressure=0,
                proximity=0,
                serial_version_uid="dummy",
                temperature=0,
            ),
            processor.PointData(
                latitude=43.8401521733766,
                longitude=10.5085820197916,
                accuracy=0.0,
                speed=15.0,
                timestamp=dt.datetime(
                    2018, 9, 13, 9, 29, 52, 79000, tzinfo=pytz.utc),
                vehicle_type=VehicleType.bus,
                session_id=1537193729,
                acceleration_x=0,
                acceleration_y=0,
                acceleration_z=0,
                battery_consumption_per_hour=0,
                battery_level=0,
                device_bearing=0,
                device_pitch=0,
                device_roll=0,
                elevation=0,
                gps_bearing=0,
                humidity=0,
                lumen=0,
                pressure=0,
                proximity=0,
                serial_version_uid="dummy",
                temperature=0,
            ),
        ],
        id="sorted inputs"
    ),
    pytest.param(
        (
                "\n"
                "0,0,0,0,0,0,0,0,0,0,0,0,43.8401521733766,10.5085820197916,0,0,0,"
                "1537193729,15,0,1536830992079,2,0\n"
                "0,0,0,0,0,0,0,0,0,0,0,0,43.8401082380725,10.5084723757338,0,0,"
                "0,1537193729,15,0,1536830988728,2,0\n"
        ),
        [
            processor.PointData(
                latitude=43.8401082380725,
                longitude=10.5084723757338,
                accuracy=0.0,
                speed=15.0,
                timestamp=dt.datetime(
                    2018, 9, 13, 9, 29, 48, 728000, tzinfo=pytz.utc),
                vehicle_type=VehicleType.bus,
                session_id=1537193729,
                acceleration_x=0,
                acceleration_y=0,
                acceleration_z=0,
                battery_consumption_per_hour=0,
                battery_level=0,
                device_bearing=0,
                device_pitch=0,
                device_roll=0,
                elevation=0,
                gps_bearing=0,
                humidity=0,
                lumen=0,
                pressure=0,
                proximity=0,
                serial_version_uid="dummy",
                temperature=0,
            ),
            processor.PointData(
                latitude=43.8401521733766,
                longitude=10.5085820197916,
                accuracy=0.0,
                speed=15.0,
                timestamp=dt.datetime(
                    2018, 9, 13, 9, 29, 52, 79000, tzinfo=pytz.utc),
                vehicle_type=VehicleType.bus,
                session_id=1537193729,
                acceleration_x=0,
                acceleration_y=0,
                acceleration_z=0,
                battery_consumption_per_hour=0,
                battery_level=0,
                device_bearing=0,
                device_pitch=0,
                device_roll=0,
                elevation=0,
                gps_bearing=0,
                humidity=0,
                lumen=0,
                pressure=0,
                proximity=0,
                serial_version_uid="dummy",
                temperature=0,
            ),
        ],
        id="non sorted inputs"
    ),
    pytest.param(
        (
            "\n"
            "0,0,0,0,0,0,0,0,0,0,0,0,43.8401521733766,10.5085820197916,0,0,0,"
            "1537193729,15,0,15368309920790000,2,0\n"
        ),
        [],
        id="faulty line (timestamp out of range) is discarded"
    ),
    pytest.param(
        (
                "\n"
                "0,0,0,0,0,0,0,0,0,0,0,0,100,10.5085820197916,0,0,0,"
                "1537193729,15,0,1536830992079,1000,0\n"
        ),
        [],
        id="faulty line (invalid vehicle type) is discarded"
    ),
    pytest.param(
        (
                "\n"
                "0,0,0,0,0,0,0,0,0,0,0,0,100,10.5085820197916,0,0,0,"
                "abcd,15,0,1536830992079,2,0\n"
        ),
        [],
        id="faulty line (invalid session_id) is discarded"
    ),

])
def test_parse_point_raw_data(raw_data, expected):
    result = processor.parse_point_raw_data(raw_data)
    for index, parsed_point in enumerate(result):
        expected_point = expected[index]
        assert parsed_point.latitude == expected_point.latitude
        assert parsed_point.longitude == expected_point.longitude
        assert parsed_point.accuracy == expected_point.accuracy
        assert parsed_point.speed == expected_point.speed
        assert parsed_point.timestamp == expected_point.timestamp
        assert parsed_point.vehicle_type == expected_point.vehicle_type
        assert parsed_point.session_id == expected_point.session_id


@pytest.mark.parametrize("points", [
    pytest.param(
        [],
        marks=pytest.mark.raises(exception=exceptions.NonRecoverableError),
        id="Raises due to not enough points",
    ),
    pytest.param(
        [
            processor.PointData(
                latitude=0,
                longitude=0,
                accuracy=0,
                speed=0,
                timestamp=dt.datetime(2018, 9, 13),
                vehicle_type=VehicleType.bus,
                session_id=1,
                acceleration_x=0,
                acceleration_y=0,
                acceleration_z=0,
                battery_consumption_per_hour=0,
                battery_level=0,
                device_bearing=0,
                device_pitch=0,
                device_roll=0,
                elevation=0,
                gps_bearing=0,
                humidity=0,
                lumen=0,
                pressure=0,
                proximity=0,
                serial_version_uid="dummy",
                temperature=0,
            ),
            processor.PointData(
                latitude=0,
                longitude=0,
                accuracy=0,
                speed=0,
                timestamp=dt.datetime(2018, 9, 13),
                vehicle_type=VehicleType.bus,
                session_id=2,
                acceleration_x=0,
                acceleration_y=0,
                acceleration_z=0,
                battery_consumption_per_hour=0,
                battery_level=0,
                device_bearing=0,
                device_pitch=0,
                device_roll=0,
                elevation=0,
                gps_bearing=0,
                humidity=0,
                lumen=0,
                pressure=0,
                proximity=0,
                serial_version_uid="dummy",
                temperature=0,
            ),
        ],
        marks=pytest.mark.raises(exception=exceptions.NonRecoverableError),
        id="Raises due to multiple session ids",
    ),
    pytest.param(
        [
            processor.PointData(
                latitude=0,
                longitude=0,
                accuracy=0,
                speed=0,
                timestamp=dt.datetime(2018, 9, 13),
                vehicle_type=VehicleType.bus,
                session_id=1,
                acceleration_x=0,
                acceleration_y=0,
                acceleration_z=0,
                battery_consumption_per_hour=0,
                battery_level=0,
                device_bearing=0,
                device_pitch=0,
                device_roll=0,
                elevation=0,
                gps_bearing=0,
                humidity=0,
                lumen=0,
                pressure=0,
                proximity=0,
                serial_version_uid="dummy",
                temperature=0,
            ),
            processor.PointData(
                latitude=0,
                longitude=0,
                accuracy=0,
                speed=0,
                timestamp=dt.datetime(2018, 9, 13),
                vehicle_type=VehicleType.bus,
                session_id=1,
                acceleration_x=0,
                acceleration_y=0,
                acceleration_z=0,
                battery_consumption_per_hour=0,
                battery_level=0,
                device_bearing=0,
                device_pitch=0,
                device_roll=0,
                elevation=0,
                gps_bearing=0,
                humidity=0,
                lumen=0,
                pressure=0,
                proximity=0,
                serial_version_uid="dummy",
                temperature=0,
            ),
        ],
        id="all good",
    ),
])
def test_validate_points(points):
    processor.validate_points(points)
