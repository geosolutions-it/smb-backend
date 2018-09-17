#########################################################################
#
# Copyright 2018, GeoSolutions Sas.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.
#
#########################################################################

"""Convert geojson files used for generating test track data to csv format.

After files are converted to ``csv`` they can be ingested by running the code
from smb-backend

"""

import argparse
import datetime as dt
import json
import logging
import pathlib
import random
from typing import List

import dateutil.parser
import pytz

logger = logging.getLogger(__name__)

CSV_FIELDS = [
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


def convert_geojson_to_csv(geojson_path: pathlib.Path,
                           target_dir: pathlib.Path):
    logger.debug("Extracting information from geojson file...")
    csv_contents = generate_csv_contents(geojson_path)
    target_dir.mkdir(parents=True, exist_ok=True)
    for track_id, track_contents in csv_contents.items():
        target_path = target_dir / "track_{}.csv".format(track_id)
        logger.debug("Writing csv file {}...".format(target_path.name))
        with target_path.open("w", encoding="utf-8") as fh:
            fh.write(track_contents)
    print("Done!")


def generate_csv_contents(geojson_path: pathlib.Path):
    with geojson_path.open() as fh:
        geojson = fh.read()
    parsed = json.loads(geojson)
    csv_header = ",".join(CSV_FIELDS)
    tracks = {}
    session_ids = {}
    for feature in parsed["features"]:
        track_id = feature["properties"]["tr"]
        if not tracks.get(track_id):
            session_ids[track_id] = int(dt.datetime.now(pytz.utc).timestamp()) + random.randint(0, 10000)
        track_points = tracks.setdefault(track_id, csv_header)
        point = extract_point(feature, session_ids[track_id])
        csv_line = ",".join([str(i) for i in point])
        tracks[track_id] = "\n".join((track_points, csv_line))
    return tracks


def extract_point(feature: dict, session_id: int) -> List:
    indexes = {field: index for index, field in enumerate(CSV_FIELDS)}
    coords = feature["geometry"]["coordinates"]
    properties = feature["properties"]
    timestamp = pytz.utc.localize(
        dateutil.parser.parse(properties["ts"], ignoretz=True)
    )
    point = [0] * len(CSV_FIELDS)
    point[indexes["longitude"]] = coords[0]
    point[indexes["latitude"]] = coords[1]
    point[indexes["sessionId"]] = session_id
    point[indexes["timeStamp"]] = int(timestamp.timestamp() * 1000)
    point[indexes["vehicleMode"]] = _convert_vehicle_type(properties["vt"])
    return point


def _convert_vehicle_type(vehicle_type):
    return {
        "foot": 1,
        "bike": 2,
        "bus": 3,
        "car": 4,
        "average_motorbike": 5,
        "train": 6,
    }.get(vehicle_type, 7)


def get_parser():
    logger = argparse.ArgumentParser(description=__doc__)
    logger.add_argument("geojson_path")
    logger.add_argument("csv_dir")
    logger.add_argument(
        "--verbose",
        action="store_true"
    )
    return logger


if __name__ == "__main__":
    parser = get_parser()
    args = parser.parse_args()
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.WARNING)
    geojson_path = pathlib.Path(args.geojson_path).expanduser()
    csv_dir = pathlib.Path(args.csv_dir).expanduser()
    convert_geojson_to_csv(
        pathlib.Path(geojson_path), pathlib.Path(csv_dir))
