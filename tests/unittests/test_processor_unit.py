#########################################################################
#
# Copyright 2018, GeoSolutions Sas.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.
#
#########################################################################

import pytest

pytestmark = pytest.mark.unit

from smbbackend import processor
from smbbackend._constants import VehicleType
from smbbackend import exceptions


@pytest.mark.parametrize("vehicle_type, duration, thresholds, expected", [
    (VehicleType.foot, 2, {VehicleType.foot: 1}, False),
    (VehicleType.foot, 0.5, {VehicleType.foot: 1}, True),
    (VehicleType.bike, 2, {VehicleType.bike: 1}, False),
    (VehicleType.bike, 0.5, {VehicleType.bike: 1}, True),
])
def test_validate_segment_duration(vehicle_type, duration, thresholds,
                                   expected):
    info = processor.SegmentInfo(
        geometry=None,
        start_date=None,
        end_date=None,
        duration=duration,
        length=None,
        average_speed=None,
        max_speed=None,
        min_speed=None,
        vehicle_type=vehicle_type
    )
    if expected:
        processor.validate_segment_duration(info, thresholds)
    else:
        with pytest.raises(exceptions.RecoverableError):
            processor.validate_segment_duration(info, thresholds)
