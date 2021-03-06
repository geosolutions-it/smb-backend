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
from . import processor
from .processor import DATA_PROCESSING_PARAMETERS
from . import calculateindexes
from .exceptions import NonRecoverableError
from . import updatebadges
from . import utils

logger = logging.getLogger(__name__)

DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")


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
            parsed_points = processor.parse_point_raw_data(csv_contents)
            session_id = processor.get_session_id(parsed_points)
            with connection as conn:
                with conn.cursor() as cursor:
                    try:
                        segments_data = processor.process_data(
                            parsed_points,
                            cursor,
                            **DATA_PROCESSING_PARAMETERS
                        )
                        validation_errors = [s[2] for s in segments_data]
                    except NonRecoverableError:
                        logger.exception(
                            "Could not process item {}".format(item))
                        continue
                    track_id = processor.save_track(
                        session_id, segments_data, args.owner_uuid, cursor)
                    utils.update_track_info(track_id, cursor)
                    track_info = utils.get_track_info(track_id, cursor)
                    if track_info.is_valid:
                        logger.info("Calculating indexes...")
                        calculateindexes.calculate_indexes(track_id, cursor)
                        logger.info("Updating badges...")
                        updatebadges.update_badges(track_id, cursor)
                    else:
                        logger.warning(
                            "track {} is not valid, so no further "
                            "calculations have been made. Validation "
                            "errors: {}".format(track_id, validation_errors)
                        )
    logger.info("Done!")


if __name__ == "__main__":
    main()
