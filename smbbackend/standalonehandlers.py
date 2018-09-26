#########################################################################
#
# Copyright 2018, GeoSolutions Sas.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.
#
#########################################################################

"""Standalone script for handling smb backend tasks

For now, this module can be run with the following (clunky) invocation:

```
DB_HOST="<host>" \
DB_NAME="<dbname>" \
DB_USER="<dbuser>" \
DB_PASSWORD="<password>" \
DB_PORT="<port>" \
python -m smbbackend.standalonehandlers \
    ~/dev/smb-backend/tests/data/track_points.csv \
    "<owner uuid>"
```

"""


import argparse
import logging
import os
import pathlib
import re

from . import ingesttracks
from . import calculateindexes
from . import updatebadges
from . import utils

logger = logging.getLogger(__name__)

DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")


def ingest_track(raw_data: str, owner_uuid: str, db_connection):
    parsed_data = ingesttracks.parse_track_data(raw_data)
    logger.debug("Performing calculations and creating database records...")
    with db_connection:  # changes are committed when `with` block exits
        with db_connection.cursor() as cursor:
            user_id = ingesttracks.get_track_owner_internal_id(
                owner_uuid, cursor)
            track_id = ingesttracks.insert_track(parsed_data, user_id, cursor)
            ingesttracks.insert_collected_points(track_id, parsed_data, cursor)
            ingesttracks.insert_segments(track_id, owner_uuid, cursor)
    return track_id


def calculate_indexes(track_identifier: int, db_connection):
    calculateindexes.calculate_indexes(track_identifier, db_connection)


def update_badges(track_identifier: int, db_connection):
    updatebadges.update_badges(track_identifier, db_connection)


def get_parser():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("csv_dir")
    parser.add_argument("owner_uuid")
    parser.add_argument(
        "--verbose",
        action="store_true"
    )
    return parser


if __name__ == "__main__":
    parser = get_parser()
    args = parser.parse_args()
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.WARNING)
    csv_dir = pathlib.Path(args.csv_dir).expanduser()
    connection = utils.get_db_connection(
        DB_NAME, DB_USER, DB_PASSWORD, DB_HOST, DB_PORT)
    for item in csv_dir.iterdir():
        if item.is_file():
            with item.open() as fh:
                logger.debug("Ingesting file {}...".format(item.name))
                csv_contents = fh.read()
                track_id = ingest_track(csv_contents, args.owner_uuid, connection)
                logger.debug("Calculating indexes...")
                calculate_indexes(track_id, connection)
                update_badges(track_id, connection)
    print("Done!")
