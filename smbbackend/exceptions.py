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


