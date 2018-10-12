#########################################################################
#
# Copyright 2018, GeoSolutions Sas.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.
#
#########################################################################

from collections import namedtuple
from collections import OrderedDict
import datetime as dt
import io
import logging
import math
from typing import List
import zipfile

import boto3
from osgeo import ogr
from osgeo import osr
import pytz

from ._constants import VehicleType

logger = logging.getLogger(__name__)

SegmentData = List[List["PointData"]]

SegmentInfo = namedtuple("SegmentInfo", [
    "geometry",
    "duration",
    "length",
    "average_speed",
    "max_speed",
    "min_speed",
    "vehicle_type"
])

class PointData(object):

    geometry: ogr.Geometry = None
    session_id: int = 0
    timestamp: dt.datetime = None
    vehicle_type: VehicleType = None

    def __init__(self, latitude: float, longitude: float,
                 timestamp: dt.datetime, vehicle_type: VehicleType,
                 session_id: int):
        self.timestamp = timestamp
        self.vehicle_type = vehicle_type
        self.session_id = session_id
        self.geometry = ogr.Geometry(ogr.wkbPoint)
        self.geometry.AddPoint(longitude, latitude)

    def __str__(self):
        return (
            "Point(lat={}, lon={}, timestamp={}, session_id={}, "
            "vehicle_type={})".format(
                self.latitude,
                self.longitude,
                self.timestamp.isoformat(),
                self.session_id,
                self.vehicle_type.name
            )
        )

    @classmethod
    def from_raw_point(cls, raw_point: str):
        info = [i.strip() for i in raw_point.split(",")]
        return cls(
            latitude=float(info[12]),
            longitude=float(info[13]),
            timestamp=dt.datetime.fromtimestamp(
                int(info[20]) / 1000).replace(tzinfo=pytz.utc),
            vehicle_type=VehicleType(int(info[21])),
            session_id=int(info[17])
        )

    @property
    def longitude(self):
        return self.geometry.GetPoint()[0]

    @property
    def latitude(self):
        return self.geometry.GetPoint()[1]

    def get_distance(self, other_point:"PointData",
                     coordinate_transformer=None):
        if coordinate_transformer is not None:
            cloned_self_geom = self.geometry.Clone()
            cloned_self_geom.Transform(coordinate_transformer)
            cloned_other_geom = other_point.geometry.Clone()
            cloned_other_geom.Transform(coordinate_transformer)
            result = cloned_self_geom.Distance(cloned_other_geom)
        else:
            result = self.geometry.Distance(other_point.geometry)
        return result


def get_coordinate_transformer():
    source_spatial_reference = osr.SpatialReference()
    source_spatial_reference.ImportFromEPSG(4326)
    distance_calculations_spatial_reference = osr.SpatialReference()
    distance_calculations_spatial_reference.ImportFromEPSG(3857)
    coordinate_transformer = osr.CoordinateTransformation(
        source_spatial_reference,
        distance_calculations_spatial_reference
    )
    return coordinate_transformer


def get_data_from_s3(bucket_name: str, object_key: str,
                     encoding: str="utf-8") -> str:
    """Download track data file from S3 and return the data

    Track data is uploaded to S3 as a zip file. This function will download
    and unzip the data

    """

    s3 = boto3.resource("s3")
    obj = s3.Object(bucket_name, object_key)
    response = obj.get()
    input_buffer = io.BytesIO(response["Body"].read())
    # TODO: do some integrity checks to the data
    result = ""
    with zipfile.ZipFile(input_buffer) as zip_handler:
        for member_name in zip_handler.namelist():
            result += zip_handler.read(member_name).decode(encoding)
    return result


def parse_point_raw_data(data: str):
    points = []
    for index, line in enumerate(data.splitlines()):
        if index > 0 and line != "":  # ignoring first line, it is file header
            try:
                point = PointData.from_raw_point(line)
            except (IndexError, ValueError):
                logger.exception("Could not parse line {}".format(index))
            else:
                points.append(point)
    return points


def validate_point_data(points: List[PointData]):
    error = None
    session_ids = set(pt.session_id for pt in points)
    if len(points) == 0:
        error = "There are no valid points in input data"
    elif len(session_ids) != 1:
        error = "Multiple session identifiers present in input data"
    return error


def generate_segments(points: List[PointData], transformer,
                      minute_threshold: int=5,
                      distance_threshold: float =20) -> List[List[PointData]]:
    """Split the input points into segments

    A new segment is created whenever some characteristic of the current
    point is significantly different from the previous one:

    - vehicle  type has changed

    - too much time has passed, with the threshold being the value of the
      ``minute_threshold`` parameter

    - current point is too far from the previous one, with the distance
      threshold being the value of the ``distance_threshold`` parameter,
      expressed in meters

    """

    segments = [[points[0]]]
    for pt in points[1:]:
        last_point = segments[-1][-1]
        vehicle_changed = pt.vehicle_type != last_point.vehicle_type
        minutes_passed = (pt.timestamp - last_point.timestamp).seconds / 60
        too_much_time_passed = minutes_passed > minute_threshold
        last_distance = pt.get_distance(last_point, transformer)
        too_far_away = last_distance > distance_threshold

        if vehicle_changed:
            logger.debug("vehicle type changed, starting new segment...")
            start_new_segment = True
        elif too_much_time_passed:
            logger.debug("too much time has passed, starting new segment...")
            start_new_segment = True
        elif too_far_away:
            logger.debug("too far away, starting new segment...")
            start_new_segment = True
        else:
            start_new_segment = False
        if start_new_segment:
            segments.append([pt])  # start new segment
        else:
            segments[-1].append(pt)

        # if vehicle_changed or too_much_time_passed or too_far_away:
        #     segments.append([pt])  # start new segment
        # else:  # continue with the same segment
        #     segments[-1].append(pt)
    return segments


def filter_invalid_temporal_points(segments: SegmentData,
                                   lower_bound=None,
                                   upper_bound=None) -> SegmentData:
    lower_bound = lower_bound or dt.datetime(2018, 1, 1, tzinfo=pytz.utc)
    upper_bound = upper_bound or dt.datetime.now(pytz.utc)

    def _check_temporal_bounds(point: PointData):
        ts = point.timestamp
        return lower_bound <= ts <= upper_bound

    return _reconcile_segments(segments, test_func=_check_temporal_bounds)


def filter_points_outside_region_of_interest(segments: SegmentData,
                                             region_of_interest: ogr.Geometry):

    def _check_intersection(point: PointData):
        return region_of_interest.Intersects(point.geometry)

    return _reconcile_segments(segments, _check_intersection)


def filter_small_segments(segments, threshold=1):
    return [s for s in segments if len(s) > threshold]


def _reconcile_segments(segments: SegmentData, test_func):
    result = []
    for segment in segments:
        new_segment = []
        for point in segment:
            if test_func(point):
                new_segment.append(point)
            else:  # discard this point and start a new segment
                logger.debug("Discarding invalid point...")
                if len(new_segment) > 0:
                    result.append(new_segment[:])
                    new_segment.clear()
        if len(new_segment) > 0:
            result.append(new_segment)
    return result


def get_region_of_interest(db_cursor):
    query = """
    WITH dumped AS (
      SELECT (ST_Dump(geom)).geom AS geom
      FROM tracks_regionofinterest
    )
    SELECT ST_AsBinary(ST_Collect(geom)) AS geom
    FROM dumped
    """
    db_cursor.execute(query)
    wkb = bytes(db_cursor.fetchone()[0])
    geometry = ogr.CreateGeometryFromWkb(wkb)
    return geometry


def get_segment_duration(segment: List[PointData]):
    temporal_delta = segment[-1].timestamp - segment[0].timestamp
    duration_seconds = (
            temporal_delta.days * 24 * 60 * 60 + temporal_delta.seconds)
    return duration_seconds


def get_segment_geometry(segment: List[PointData]):
    linestring_geom = ogr.Geometry(ogr.wkbLineString)
    for point in segment:
        linestring_geom.AddPoint(point.longitude, point.latitude)
    return linestring_geom


def get_length(linestring_geom: ogr.Geometry, coordinate_transformer):
    cloned_geom = linestring_geom.Clone()
    cloned_geom.Transform(coordinate_transformer)
    return cloned_geom.Length()


def get_segment_speeds(segment, coordinate_transformer):
    """Return the maximum and minimum speed of the segment

    Speed is calculated as the rate of change in position of adjacent points

    """

    max_speed = 0
    min_speed = 1000
    for p1_index in range(0, len(segment), 2):
        p2_index = p1_index + 1
        p1 = segment[p1_index]
        p2 = segment[p2_index]
        sub_segment = [p1, p2]
        sub_segment_duration = get_segment_geometry(sub_segment)
        geom = get_segment_geometry(sub_segment)
        sub_segment_length = get_length(geom, coordinate_transformer)
        average_speed = sub_segment_length / sub_segment_duration
        max_speed = max(max_speed, average_speed)
        min_speed = max(min_speed, average_speed)
    return max_speed, min_speed


def get_segment_info(segment: List[PointData], coordinate_transformer):
    duration = get_segment_duration(segment)
    segment_geometry = get_segment_geometry(segment)
    length = get_length(segment_geometry, coordinate_transformer)
    average_speed = length / duration  # expressed in m/s
    max_speed, min_speed = get_segment_speeds(segment, coordinate_transformer)
    return SegmentInfo(
        geometry=segment_geometry,
        duration=duration,
        length=length,
        average_speed=average_speed,
        max_speed=max_speed,
        min_speed=min_speed,
        vehicle_type=segment[0].vehicle_type
    )


def verify_segment_consistency(segment: List[PointData], info: SegmentInfo):
    speed_verification = verify_segment_speed(info)
    return all(speed_verification)


def verify_segment_speed(segment_info: SegmentInfo):
    # these are expressed in m/s
    speed_thresholds = {  # (average_speed, max_speed)
        VehicleType.foot: (2.7, 2.7),
        VehicleType.bike: (8.3, 8.3),
        VehicleType.motorcycle: (22.2, 22.2),
        VehicleType.car: (22.2, 22.2),
        VehicleType.bus: (16.7, 16.7),
        VehicleType.train: (22.2, 22.2),
    }
    avg_threshold, max_threshold = speed_thresholds[segment_info.vehicle_type]
    average_speed_valid = segment_info.average_speed <= avg_threshold
    max_speed_valid = segment_info.max_speed <= max_threshold
    return average_speed_valid, max_speed_valid


def validate_segments(segments: SegmentData, coordinate_transformer):
    if len(segments) == 0:
        error = "No valid segments left after filtering"
    else:
        for segment in segments:
            info = get_segment_info(segment, coordinate_transformer)
            verify_segment_consistency(segment, info)


def process_data(raw_data, db_cursor, distance_threshold=20):
    logger.info("parsing raw data...")
    point_data = parse_point_raw_data(raw_data)
    error = validate_point_data(point_data)
    if error is not None:
        pass  # interrupt processing
    coordinate_transformer = get_coordinate_transformer()
    segments = generate_segments(
        point_data,
        coordinate_transformer,
        distance_threshold=distance_threshold
    )
    region_of_interest = get_region_of_interest(db_cursor)
    segment_filterers = OrderedDict({
        filter_invalid_temporal_points: (
            None,
            {
                "lower_bound": dt.datetime(2018, 1, 1, tzinfo=pytz.utc),
                "upper_bound": dt.datetime.now(pytz.utc)
            }
        ),
        filter_points_outside_region_of_interest: (
            (region_of_interest, ),
            None
        ),
        filter_small_segments: (
            None,
            {"threshold": 1}
        ),
    })
    segments = apply_segment_filterers(segments, segment_filterers)
    validate_segments(segments, coordinate_transformer)
    return segments


def apply_segment_filterers(segments, filterers):
    current_segments = segments
    for handler, params in filterers.items():
        args, kwargs = params
        current_segments = handler(current_segments, *args, **kwargs)
        if len(current_segments) == 0:
            break
    return current_segments
