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
Convert shapefiles into suitable csv files that can be ingested into the DB

"""

import argparse
from collections import namedtuple
from collections import OrderedDict
import datetime as dt
import os
from pathlib import Path
import random

from osgeo import ogr
from osgeo import osr
import pytz

from ._constants import VehicleType

in_path = "tracks.shp"
out_dir = "data"
out_format = "CSV"
in_crs = 3857
out_crs = 4326

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

source = osr.SpatialReference()
source.ImportFromEPSG(in_crs)
target = osr.SpatialReference()
target.ImportFromEPSG(out_crs)
transform = osr.CoordinateTransformation(source, target)


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

    def distance(self, point):
        return self.geom.Distance(point.geom)

    def __str__(self):
        return "Point {} ({})".format(
            self.id,
            self.timestamp
        )


class Track(object):

    def __init__(self, track_id):
        self.track_id = track_id
        self.session_id = int(
            dt.datetime.now(pytz.utc).timestamp()) + random.randint(0, 10000)
        self.segments = []

    def add_segment(self, segment):
        self.segments.append(segment)

    def prepare(self):
        cumtime = 0
        for segment in self.segments:
            for i, trackedpoint in enumerate(segment.trackedpoints):

                trackedpoint.id = i
                if i == 0:
                    distance = 0.0
                    timestamp = segment.timestamp
                else:
                    distance = abs(
                        trackedpoint.distance(segment.get_point(i - 1)))
                    cumtime += (distance / segment.speed)
                    timestamp = segment.timestamp + dt.timedelta(
                        seconds=cumtime)
                trackedpoint.timestamp = timestamp
                trackedpoint.fields_values.update({
                    "sessionId": self.session_id,
                    "vehicleMode": segment.vehicle_type.value,
                    "longitude": trackedpoint.x,
                    "latitude": trackedpoint.y,
                    "speed": VEHICLE_SPEED.get(segment.vehicle_type, 0),
                    "timeStamp": int(timestamp.timestamp() * 1000),
                })

    def serialize(self, out_path):
        Path(out_path).mkdir(parents=True, exist_ok=True)
        dest = os.path.abspath(
            os.path.join(out_path, "track_{}.csv".format(self.track_id)))
        out_driver = ogr.GetDriverByName(out_format)
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

                if in_crs != out_crs:
                    trackedpoint.geom.Transform(transform)
                    trackedpoint.fields_values['longitude'] = trackedpoint.x
                    trackedpoint.fields_values['latitude'] = trackedpoint.y

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


def make_tracks(layer: ogr.Layer) -> OrderedDict:
    # this style of feature iteration is a workaround for:
    #   https://github.com/nextgis/pygdal/issues/31
    tracks = OrderedDict()
    feature = layer.GetNextFeature()
    while feature is not None:
        print("feature: {}".format(feature.GetField("ID")))
        track_id = feature.GetField('TRACK_ID')
        track = tracks.setdefault(track_id, Track(track_id))
        segment_data = SegmentsData(
            feature.GetField('SEGMENT_ID'),
            feature.GetField('VEHICLE_TY'),
            dt.datetime.strptime(
                feature.GetField("TIMESTAMP"),
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
    parser.add_argument("shapefile_path")
    parser.add_argument(
        "-o", "--out_dir", default=Path(__file__).parent.resolve())
    parser.add_argument(
        "--verbose",
        action="store_true"
    )
    return parser


def main():
    parser = get_parser()
    args = parser.parse_args()
    shapefile = args.shapefile_path
    out_dir = args.out_dir
    if not os.path.exists(shapefile):
        raise SystemExit("Could not find file {!r}".format(shapefile))
    ogr_driver = ogr.GetDriverByName("ESRI Shapefile")
    data_source = ogr_driver.Open(shapefile, 0)  # 0 means read-only
    if data_source is None:
        raise SystemExit("Could not open {!r} as a shapefile".format(shapefile))
    layer = data_source.GetLayer(0)
    tracks = make_tracks(layer)
    for track in tracks.values():
        track.serialize(out_dir)


if __name__ == "__main__":
    main()
