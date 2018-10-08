#########################################################################
#
# Copyright 2018, GeoSolutions Sas.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.
#
#########################################################################

"""
Convert spatialite table rows into suitable csv files that can be ingested
into the DB

"""

import argparse
from collections import namedtuple
from collections import OrderedDict
import datetime as dt
import logging
import os
from pathlib import Path
import random
from typing import List

from osgeo import ogr
from osgeo import osr
import pytz

from ._constants import VehicleType

logger = logging.getLogger(__name__)

VEHICLE_SPEED = {
    VehicleType.foot: 4.0,
    VehicleType.bike: 15.0,
    VehicleType.motorcycle: 50.0,
    VehicleType.car: 50.0,
    VehicleType.bus: 40.0,
    VehicleType.train: 90.0
}


CsvField = namedtuple("CsvField", [
    "name",
    "ogr_type"
])

csv_fields = [
    CsvField("accelerationX", ogr.OFTReal),
    CsvField("accelerationY", ogr.OFTReal),
    CsvField("accelerationZ", ogr.OFTReal),
    CsvField("accuracy", ogr.OFTReal),
    CsvField("batConsumptionPerHour", ogr.OFTReal),
    CsvField("batteryLevel", ogr.OFTReal),
    CsvField("deviceBearing", ogr.OFTReal),
    CsvField("devicePitch", ogr.OFTReal),
    CsvField("deviceRoll", ogr.OFTReal),
    CsvField("elevation", ogr.OFTReal),
    CsvField("gps_bearing", ogr.OFTReal),
    CsvField("humidity", ogr.OFTReal),
    CsvField("latitude", ogr.OFTReal),
    CsvField("longitude", ogr.OFTReal),
    CsvField("lumen", ogr.OFTReal),
    CsvField("pressure", ogr.OFTReal),
    CsvField("proximity", ogr.OFTReal),
    CsvField("sessionId", ogr.OFTInteger),
    CsvField("speed", ogr.OFTReal),
    CsvField("temperature", ogr.OFTReal),
    CsvField("timeStamp", ogr.OFTString),
    CsvField("vehicleMode", ogr.OFTInteger),
    CsvField("serialVersionUID", ogr.OFTInteger),
]


class TrackedPoint():

    def __init__(self, geom = None):
        self.geom = geom
        self.id = None
        self.timestamp = None
        self.fields_values = {
            "accelerationX": 0.0,
            "accelerationY": 0.0,
            "accelerationZ": 0.0,
            "accuracy": 0.0,
            "batConsumptionPerHour": 0.0,
            "batteryLevel": 0.0,
            "deviceBearing": 0.0,
            "devicePitch": 0.0,
            "deviceRoll": 0.0,
            "elevation": 0.0,
            "gps_bearing": 0.0,
            "humidity": 0.0,
            "latitude": 0.0,
            "longitude": 0.0,
            "lumen": 0.0,
            "pressure": 0.0,
            "proximity": 0.0,
            "sessionId": 0,
            "speed": 0.0,
            "temperature": 0.0,
            "timeStamp": 0,
            "vehicleMode": 0,
            "serialVersionUID": 0,
        }

    @property
    def x(self):
        return self.geom.GetX()

    @property
    def y(self):
        return self.geom.GetY()

    def get_distance(self, other_point, coordinate_transformer=None):
        if coordinate_transformer is not None:
            cloned_self_geom = self.geom.Clone()
            cloned_self_geom.Transform(coordinate_transformer)
            cloned_other_geom = other_point.geom.Clone()
            cloned_other_geom.Transform(coordinate_transformer)
            result = cloned_self_geom.Distance(cloned_other_geom)
        else:
            result = self.geom.Distance(other_point.geom)
        return result

    def __str__(self):
        return "Point {} ({})".format(
            self.id,
            self.timestamp
        )


class Track(object):

    def __init__(self, track_id, coordinate_transformer=None):
        self.track_id = track_id
        self.session_id = int(
            dt.datetime.now(pytz.utc).timestamp()) + random.randint(0, 10000)
        self.segments = []
        self.coordinate_transformer = coordinate_transformer

    def add_segment(self, segment):
        self.segments.append(segment)

    def prepare(self):
        cumtime = 0
        for segment in self.segments:
            for index, pt in enumerate(segment.trackedpoints):

                pt.id = index
                if index == 0:
                    distance = 0.0
                    timestamp = segment.timestamp
                else:
                    distance = abs(
                        pt.get_distance(
                            segment.get_point(index - 1),
                            coordinate_transformer=self.coordinate_transformer
                        )
                    )
                    cumtime += (distance / segment.speed)
                    timestamp = segment.timestamp + dt.timedelta(
                        seconds=cumtime)
                pt.timestamp = timestamp
                logger.debug("pt {}: {}".format(pt.id, pt.timestamp))
                pt.fields_values.update({
                    "sessionId": self.session_id,
                    "vehicleMode": segment.vehicle_type.value,
                    "longitude": pt.x,
                    "latitude": pt.y,
                    "speed": VEHICLE_SPEED.get(segment.vehicle_type, 0),
                    "timeStamp": int(timestamp.timestamp() * 1000),
                })

    def serialize(self, out_path):
        Path(out_path).mkdir(parents=True, exist_ok=True)
        dest = os.path.abspath(
            os.path.join(out_path, "track_{}.csv".format(self.track_id)))
        out_driver = ogr.GetDriverByName("CSV")
        out_datasource = out_driver.CreateDataSource(dest)
        out_layer = out_datasource.CreateLayer(
            "waypoints", geom_type=ogr.wkbPoint)

        for csv_field in csv_fields:
            field = ogr.FieldDefn(csv_field.name, csv_field.ogr_type)
            out_layer.CreateField(field)
            field = None

        feature_defn = out_layer.GetLayerDefn()

        for segment in self.segments:
            for trackedpoint in segment.trackedpoints:
                feature = ogr.Feature(feature_defn)
                for fieldName, value in trackedpoint.fields_values.items():
                    feature.SetField(fieldName, value)

                feature.SetGeometry(trackedpoint.geom)
                out_layer.CreateFeature(feature)
                feature = None

        outDataSource = None


class SegmentsData(object):

    def __init__(self, segment_id = None, vehicle_type = None,
                 timestamp = None):
        self.segment_id = segment_id
        self.vehicle_type = VehicleType[vehicle_type]
        self.timestamp = timestamp
        self.speed = VEHICLE_SPEED.get(self.vehicle_type, 0) * 1000 / 3600
        self.trackedpoints = []

    def add_point(self, point):
        self.trackedpoints.append(TrackedPoint(point))

    def get_point(self, i):
        return self.trackedpoints[i] if i < len(self.trackedpoints) else None


def make_tracks(layer: ogr.Layer, coordinate_transformer) -> OrderedDict:
    tracks = OrderedDict()
    # this (ugly) style of feature iteration is a workaround for:
    # https://github.com/nextgis/pygdal/issues/31
    feature = layer.GetNextFeature()
    while feature is not None:
        track_id = feature.GetField("track_id")
        track = tracks.setdefault(
            track_id,
            Track(track_id, coordinate_transformer)
        )
        segment_data = SegmentsData(
            segment_id=feature.GetField('segment_id'),
            vehicle_type=feature.GetField('vehicle_type'),
            timestamp=dt.datetime.strptime(
                feature.GetField("timestamp"),
                "%Y-%m-%d %H:%M:%S"
            ).replace(tzinfo=dt.timezone.utc)
        )
        geom = feature.GetGeometryRef()
        for lon, lat in geom.GetPoints():
            geom = ogr.Geometry(ogr.wkbPoint)
            geom.AddPoint(lon, lat)
            segment_data.add_point(geom)
        track.add_segment(segment_data)
        feature = layer.GetNextFeature()
    for item in tracks.values():
        item.prepare()
    return tracks


def get_parser():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "source_path",
        help="Full path to a spatialite DB or to a shapefile to use as input"
    )
    parser.add_argument(
        "-o",
        "--out_dir",
        default=Path(__file__).parent.resolve()
    )
    parser.add_argument(
        "--layer",
        help="Name of the layer to handle. Defaults to the first layer on the "
             "file"
    )
    parser.add_argument(
        "--input_epsg",
        type=int,
        default=4326,
        help="EPSG code of the input's coordinate reference system. Defaults "
             "to 4326"
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
    spatial_path = Path(args.source_path).expanduser().resolve()
    out_dir = args.out_dir
    if not spatial_path.exists():
        raise SystemExit("Could not find file {!r}".format(spatial_path))
    ogr_driver = ogr.GetDriverByName("SQLite")
    data_source = ogr_driver.Open(str(spatial_path), 0)  # 0 means read-only
    if data_source is None:
        raise SystemExit("Could not open {!r}".format(spatial_path))
    if args.layer is not None:
        layer = data_source.GetLayerByName(args.layer)
    else:
        layer = data_source.GetLayer(0)
    if layer is None:
        raise SystemExit("Could not open layer")
    source_spatial_reference = osr.SpatialReference()
    source_spatial_reference.ImportFromEPSG(args.input_epsg)
    distance_calculations_spatial_reference = osr.SpatialReference()
    distance_calculations_spatial_reference.ImportFromEPSG(3857)
    coordinate_transformer = osr.CoordinateTransformation(
        source_spatial_reference,
        distance_calculations_spatial_reference
    )
    tracks = make_tracks(layer, coordinate_transformer)
    for track in tracks.values():
        logger.info("Saving track {}...".format(track.track_id))
        track.serialize(out_dir)
    logger.info("Done!")


if __name__ == "__main__":
    main()
