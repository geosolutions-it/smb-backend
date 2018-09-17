import argparse
import os
import random
import datetime as dt
import pytz
from pathlib import Path
from collections import OrderedDict
from osgeo import ogr, osr
import fiona

in_path = "tracks.shp"
out_dir = "data"
out_format = "CSV"
in_crs = 3857
out_crs = 4326


def _vehicle_speed(vehicle_type):
    return {
        'foot': 4.0,
        'bike': 15.0,
        'motorbike': 50.0,
        'car': 50.0,
        'bus': 40.0,
        'train': 90.0
    }.get(vehicle_type, 0.0)


def _convert_vehicle_type(vehicle_type):
    return {
        "foot": 1,
        "bike": 2,
        "bus": 3,
        "car": 4,
        "average_motorbike": 5,
        "train": 6,
    }.get(vehicle_type, 7)


FIELDS = [
    {
        "name": "accelerationX",
        "type": "real"
    },
    {
        "name": "accelerationY",
        "type": "real"
    },
    {
        "name": "accelerationZ",
        "type": "real"
    },
    {
        "name": "accuracy",
        "type": "real"
    },{
        "name": "batConsumptionPerHour",
        "type": "real"
    },
    {
        "name": "batteryLevel",
        "type": "real"
    },
    {
        "name": "deviceBearing",
        "type": "real"
    },
    {
        "name": "devicePitch",
        "type": "real"
    },
    {
        "name": "deviceRoll",
        "type": "real"
    },
    {
        "name": "elevation",
        "type": "real"
    },
    {
        "name": "gps_bearing",
        "type": "real"
    },
    {
        "name": "humidity",
        "type": "real"
    },
    {
        "name": "latitude",
        "type": "real"
    },
    {
        "name": "longitude",
        "type": "real"
    },
    {
        "name": "lumen",
        "type": "real"
    },
    {
        "name": "pressure",
        "type": "real"
    },
    {
        "name": "proximity",
        "type": "real"
    },
    {
        "name": "sessionId",
        "type": "int"
    },
    {
        "name": "speed",
        "type": "real"
    },
    {
        "name": "temperature",
        "type": "real"
    },
    {
        "name": "timeStamp",
        "type": "string"
    },
    {
        "name": "vehicleMode",
        "type": "int"
    },
    {
        "name": "serialVersionUID",
        "type": "int"
    },
]

OGR_TYPES = {
    'int': ogr.OFTInteger,
    'real': ogr.OFTReal,
    "string": ogr.OFTString,
    "datetime": ogr.OFTDateTime
}


source = osr.SpatialReference()
source.ImportFromEPSG(in_crs)
target = osr.SpatialReference()
target.ImportFromEPSG(out_crs)
transform = osr.CoordinateTransformation(source, target)


class TrackedPoint():

    def __init__(self, geom = None):
        self.geom = geom
        self.id = None
        self.cumdistance = None
        self.prevdistance = None
        self.cumtime = None
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
        return "Point {} distance {} ({} / {} / {})".format(self.id,self.prevdistance,self.cumdistance,self.cumtime,self.timestamp)


class Track():
    def __init__(self, track_id):
        self.track_id = track_id
        self.session_id = int(dt.datetime.now(pytz.utc).timestamp()) + random.randint(0, 10000)
        self.segments = []

    def addSegment(self, segment):
        self.segments.append(segment)

    def prepare(self):
        cumdistance = 0.0
        cumtime = 0
        for segment in self.segments:
            for i, trackedpoint in enumerate(segment.getPoints()):

                trackedpoint.id = i
                if i == 0:
                    distance = 0.0
                    timestamp = segment.timestamp
                else:
                    distance = abs(trackedpoint.distance(segment.getPoint(i - 1)))
                    cumdistance += distance
                    cumtime += (distance / segment.speed)
                    timestamp = segment.timestamp + dt.timedelta(seconds=cumtime)

                trackedpoint.prevdistance = distance
                trackedpoint.cumdistance = cumdistance
                trackedpoint.cumtime = cumtime
                trackedpoint.timestamp = timestamp

                trackedpoint.fields_values['sessionId'] = self.session_id
                trackedpoint.fields_values['vehicleMode'] = _convert_vehicle_type(segment.vehicle_type)
                trackedpoint.fields_values['longitude'] = trackedpoint.x
                trackedpoint.fields_values['latitude'] = trackedpoint.y
                trackedpoint.fields_values['speed'] = _vehicle_speed(segment.vehicle_type)
                trackedpoint.fields_values['timeStamp'] = int(timestamp.timestamp()*1000)
            a = 1
        self.length = cumdistance

    def serialize(self, out_path):
        Path(out_path).mkdir(parents=True, exist_ok=True)
        dest = os.path.abspath(os.path.join(out_path, "track_{}.csv".format(self.track_id)))
        out_driver = ogr.GetDriverByName(out_format)
        out_datasource = out_driver.CreateDataSource(dest)
        out_layer = out_datasource.CreateLayer("waypoints", geom_type=ogr.wkbPoint)

        for field in FIELDS:
            field = ogr.FieldDefn(field["name"],OGR_TYPES[field["type"]])
            out_layer.CreateField(field)
            field = None

        feature_defn = out_layer.GetLayerDefn()

        for segment in self.segments:
            for trackedpoint in segment.getPoints():
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


class SegmentsData():

    def __init__(self,segment_id = None, vehicle_type = None, timestamp = None):
        self.segment_id = segment_id
        self.vehicle_type = vehicle_type
        self.timestamp = timestamp

        self.speed = _vehicle_speed(self.vehicle_type) * 1000/3600
        self.length = 0.0

        self.trackedpoints = []

    def addPoint(self, point):
        self.trackedpoints.append(TrackedPoint(point))

    def npoints(self):
        return len(self.trackedpoints)

    def getPoints(self):
        return self.trackedpoints

    def getPoint(self, i):
        return self.trackedpoints[i] if i < len(self.trackedpoints) else None

    def prepare(self):
        pass

    def _prepare(self):
        pass

    def serialice(self, out):
        pass


def makeTracks(source):
    tracks = OrderedDict()
    for feature in source:
        props = feature['properties']

        track_id = props['track_id']
        track = tracks.get(track_id)
        if not track:
            track = Track(track_id)
            tracks[track_id] = track

        segment_id = props['segment_id']
        vehicle_type = props['vehicle_ty']
        timestamp_s = props['timestamp']
        timestamp = dt.datetime.strptime(timestamp_s, "%Y-%m-%d %H:%M:%S").replace(tzinfo=dt.timezone.utc)

        segment_data = SegmentsData(segment_id, vehicle_type, timestamp)

        for coord in feature['geometry']['coordinates']:
            geom = ogr.Geometry(ogr.wkbPoint)
            geom.AddPoint(coord[0], coord[1])
            segment_data.addPoint(geom)

        track.addSegment(segment_data)

    for track in tracks.values():
        track.prepare()

    return tracks

def get_parser():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("shapefile", nargs='?', default=in_path)
    parser.add_argument("out_dir", nargs='?', default=out_dir)
    parser.add_argument(
        "--verbose",
        action="store_true"
    )
    return parser


if __name__ == "__main__":
    parser = get_parser()
    args = parser.parse_args()
    shapefile = args.shapefile
    out_dir = args.out_dir

    if not os.path.exists(shapefile):
        print("Can't find {}".format(shapefile))
        exit(1)

    segments_data = None
    with fiona.open(shapefile) as source:
        tracks = makeTracks(source)

    for track in tracks.values():
        track.serialize(out_dir)

