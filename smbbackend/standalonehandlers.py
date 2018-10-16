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
from pathlib import Path

# from .ingesttracks import ingest_data
from .processor import DATA_PROCESSING_PARAMETERS
from .processor import process_data
from .processor import save_track
from . import calculateindexes
from . import updatebadges
from . import utils

logger = logging.getLogger(__name__)

DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")


def calculate_indexes(track_identifier: int, db_connection):
    calculateindexes.calculate_indexes(track_identifier, db_connection)


def update_badges(track_identifier: int, db_connection):
    updatebadges.update_badges(track_identifier, db_connection)


def get_parser():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("owner_uuid")
    parser.add_argument(
        "csv_path",
        nargs="+"
    )
    parser.add_argument(
        "--verbose",
        action="store_true"
    )
    return parser


def main():
    parser = get_parser()
    args = parser.parse_args()
    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO)
    connection = utils.get_db_connection(
        DB_NAME, DB_USER, DB_PASSWORD, DB_HOST, DB_PORT)
    for item in (Path(i).expanduser().resolve() for i in args.csv_path):
        if item.is_file():
            logger.info("Ingesting file {}...".format(item.name))
            with item.open() as fh:
                csv_contents = fh.read()
            with connection as conn:
                with conn.cursor() as cursor:
                    segments = process_data(
                        csv_contents, cursor, **DATA_PROCESSING_PARAMETERS)
                    track_id = save_track(segments, args.owner_uuid, cursor)
            logger.info("Calculating indexes...")
            calculate_indexes(track_id, connection)
            logger.info("Updating badges...")
            update_badges(track_id, connection)
    logger.info("Done!")


if __name__ == "__main__":
    main()
