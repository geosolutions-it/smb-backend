#########################################################################
#
# Copyright 2018, GeoSolutions Sas.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.
#
#########################################################################

"""Custom exception classes for smbbackend"""


class NonRecoverableError(Exception):
    """Raise whenever input data is not valid and the user cannot fix it

    This exception should be raised whenever the input data has some error
    that is not fixable by the user using valid means. For example, if all
    collected points fall outside the current region of interest. In the
    previous case, since the user is not allowed to change the coordinates of
    the collected points, there is no way to fix this type of illegal data

    """

    pass


class RecoverableError(Exception):
    """Raise whenever input data is not valid but the user may fix it

    This exception should be raised whenever the input data has some error
    that is likely due to some human error and there is the possibility to
    fix it. For example, if the user uploads a track where the average speed
    is 100km/h and the declared vehicle type is `foot`, then it is likely
    that the user has made a car trip and forgot to switch the vehicle type.

    """

    def __init__(self, error_message, variable_name, value, vehicle_type,
                 *args, **kwargs):
        super().__init__(
            error_message, variable_name, value, vehicle_type, *args, **kwargs)
        self.variable_name = variable_name
        self.value = value
        self.vechile_type = vehicle_type
