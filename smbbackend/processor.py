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
import datetime as dt
from functools import partial
import io
import logging
from typing import List
from typing import Callable
from typing import Tuple
import zipfile

import boto3
import numpy as np
from osgeo import gdal
from osgeo import ogr
from osgeo import osr
import pytz

from ._constants import VehicleType
from . import exceptions
from . import utils
from .utils import get_query

gdal.UseExceptions()
logger = logging.getLogger(__name__)

# TODO: re-enable validation results
ENABLE_TRACK_VALIDATION = False

DATA_PROCESSING_PARAMETERS = {
    "segments_distance_thresholds": {  # in m
        VehicleType.foot: 50,
        VehicleType.bike: 300,
        VehicleType.motorcycle: 500,
        VehicleType.car: 500,
        VehicleType.bus: 500,
        VehicleType.train: 1000,
    },
    "segments_minute_threshold": 5,
    "segments_temporal_lower_bound": dt.datetime(2018, 1, 1, tzinfo=pytz.utc),
    "segments_temporal_upper_bound": (
        dt.datetime.now(pytz.utc) + dt.timedelta(days=1)),
    "segments_small_threshold": 1,
    "points_position_threshold": 0.1,
    "points_accuracy_threshold": 100,
    "segments_speed_thresholds": {  # (average_speed, max_speed), in m/s
        VehicleType.foot: (5.6, 5.6),  # 20km/h , 20 km/h
        VehicleType.bike: (30, 30),  # 108 km/h, 108 km/h
    },
    "segments_length_thresholds": {  # expressed in m
        VehicleType.foot: 50000,  # 50 km
        VehicleType.bike: 150000,  # 150 km
    },
    "segments_duration_thresholds": {  # expressed in seconds
        VehicleType.foot: 43200,  # 12 hours
        VehicleType.bike: 43200,  # 12 hours
    },
    "segments_pairwise_stddev_coeff": {
        VehicleType.foot: 2,
        VehicleType.bike: 2,
        VehicleType.motorcycle: 2,
        VehicleType.car: 2,
        VehicleType.bus: 2,
        VehicleType.train: 2,
    }
}

SegmentData = List[List["PointData"]]
FullSegmentData = List[Tuple[List["PointData"], "SegmentInfo", List]]

SegmentInfo = namedtuple("SegmentInfo", [
    "geometry",
    "projected_geometry",
    "start_date",
    "end_date",
    "duration",  # measured in seconds
    "length",  # in m
    "average_speed",  # measured in m/s
    "max_speed",  # measured in m/s
    "min_speed",  # measured in m/s
    "vehicle_type"
])


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


class PointData(object):

    _geometry: ogr.Geometry = None
    _longitude: float = None
    _latitude: float = None
    _x: float = None
    _y: float = None
    acceleration_x: float = None
    acceleration_y: float = None
    acceleration_z: float = None
    accuracy: float = None
    battery_consumption_per_hour: float = None
    battery_level: float = None
    coordinate_transformer = get_coordinate_transformer()
    device_bearing: float = None
    device_pitch: float = None
    device_roll: float = None
    elevation: float = None
    gps_bearing: float = None
    humidity: float = None
    lumen: float = None
    pressure: float = None
    proximity: float = None
    serial_version_uid: str = None
    session_id: int = 0
    speed: float = None
    temperature: float = None
    timestamp: dt.datetime = None
    vehicle_type: VehicleType = None

    def __init__(self, acceleration_x:float, acceleration_y: float,
                 acceleration_z:float, accuracy:float,
                 battery_consumption_per_hour: float, battery_level: float,
                 device_bearing: float, device_pitch: float,
                 device_roll: float, elevation: float, gps_bearing: float,
                 humidity: float, lumen: float, pressure: float,
                 proximity: float, serial_version_uid: str, session_id: int,
                 speed: float, temperature: float, timestamp: dt.datetime,
                 vehicle_type: VehicleType, latitude: float, longitude: float):
        self.acceleration_x: float = acceleration_x
        self.acceleration_y: float = acceleration_y
        self.acceleration_z: float = acceleration_z
        self.accuracy = accuracy
        self.battery_consumption_per_hour: float = battery_consumption_per_hour
        self.battery_level: float = battery_level
        self.device_bearing: float = device_bearing
        self.device_pitch: float = device_pitch
        self.device_roll: float = device_roll
        self.elevation: float = elevation
        self.gps_bearing: float = gps_bearing
        self.humidity: float = humidity
        self.lumen: float = lumen
        self.pressure: float = pressure
        self.proximity: float = proximity
        self.serial_version_uid = serial_version_uid
        self.session_id = session_id
        self.speed = speed
        self.temperature: float = temperature
        self.timestamp = timestamp
        self.vehicle_type = vehicle_type
        self._longitude = longitude
        self._latitude = latitude
        self._geometry = ogr.Geometry(ogr.wkbPoint)
        self._geometry.AddPoint(longitude, latitude)
        cloned_geom = self._geometry.Clone()
        cloned_geom.Transform(self.coordinate_transformer)
        self._projected_geometry = cloned_geom
        self._x, self._y = self._projected_geometry.GetPoint()[:2]

    def __str__(self):
        return (
            "Point(latitude={}, longitude={}, accuracy={}, timestamp={}, "
            "speed={}, session_id={}, vehicle_type={})".format(
                self.latitude,
                self.longitude,
                self.accuracy,
                self.timestamp.isoformat(),
                self.speed,
                self.session_id,
                self.vehicle_type.name
            )
        )

    def __repr__(self):
        return str(self)

    @classmethod
    def from_raw_point(cls, raw_point: str):
        info = [i.strip() for i in raw_point.split(",")]
        return cls(
            acceleration_x=float(info[0]),
            acceleration_y=float(info[1]),
            acceleration_z=float(info[2]),
            accuracy=float(info[3]),
            battery_consumption_per_hour=float(info[4]),
            battery_level=float(info[5]),
            device_bearing=float(info[6]),
            device_pitch=float(info[7]),
            device_roll=float(info[8]),
            elevation=float(info[9]),
            gps_bearing=float(info[10]),
            humidity=float(info[11]),
            lumen=float(info[14]),
            pressure=float(info[15]),
            proximity=float(info[16]),
            serial_version_uid=info[22],
            session_id=int(info[17]),
            speed=float(info[18]),
            temperature=float(info[19]),
            timestamp=dt.datetime.fromtimestamp(
                int(info[20]) / 1000).replace(tzinfo=pytz.utc),
            vehicle_type=VehicleType(int(info[21])),
            latitude=float(info[12]),
            longitude=float(info[13]),
        )

    @property
    def geometry(self):
        return self._geometry

    @property
    def projected_geometry(self):
        return self._projected_geometry

    @property
    def longitude(self):
        return self._longitude

    @property
    def latitude(self):
        return self._latitude

    @property
    def x(self):
        return self._x

    @property
    def y(self):
        return self._y

    def get_distance(self, other_point: "PointData"):
        return self.projected_geometry.Distance(other_point.projected_geometry)


def ingest_s3_data(s3_bucket_name: str, object_key: str, owner_uuid: str,
                   db_cursor) -> Tuple[FullSegmentData, int, int]:
    """Ingest track data into smb database"""
    logger.debug("Retrieving data from S3 bucket...")
    raw_data = get_data_from_s3(s3_bucket_name, object_key)
    points = parse_point_raw_data(raw_data)
    session_id = get_session_id(points)
    segments_data = process_data(
        points,
        db_cursor,
        **DATA_PROCESSING_PARAMETERS
    )
    track_id = save_track(session_id, segments_data, owner_uuid, db_cursor)
    utils.update_track_info(track_id, db_cursor)
    return segments_data, track_id, session_id


def save_track(session_id, segments_data: FullSegmentData, owner_uuid: str,
               db_cursor):
    owner_internal_id = get_track_owner_internal_id(owner_uuid, db_cursor)
    track_id = insert_track(session_id, owner_internal_id, segments_data,
                            db_cursor)
    insert_points(track_id, segments_data, db_cursor)
    insert_segments(track_id, segments_data, owner_uuid, db_cursor)
    return track_id


def insert_track(session_id: int, owner: int,
                 segments_data: FullSegmentData, db_cursor) -> int:
    """Insert track data into the main database"""
    track_errors = []
    for segment_data in segments_data:
        for error in segment_data[2]:
            track_errors.append(f'{error["vehicle_type"]}: {error["msg"]}')
    query = get_query("insert-track.sql")
    db_cursor.execute(
        query,
        {
            "owner_id": owner,
            "session_id": session_id,
            "created_at": dt.datetime.now(pytz.utc),
            "is_valid": is_track_valid(segments_data),
            "validation_error": ", ".join(track_errors)
        }
    )
    track_id = db_cursor.fetchone()[0]
    return track_id


def insert_points(track_id: int, segments: FullSegmentData, db_cursor):
    query = get_query("insert-point.sql")
    for segment, info, errors in segments:
        for point in segment:
            db_cursor.execute(
                query,
                {
                    "acceleration_x": point.acceleration_x,
                    "acceleration_y": point.acceleration_y,
                    "acceleration_z": point.acceleration_z,
                    "accuracy": point.accuracy,
                    "battery_consumption": point.battery_consumption_per_hour,
                    "battery_level": point.battery_level,
                    "device_bearing": point.device_bearing,
                    "device_pitch": point.device_pitch,
                    "device_roll": point.device_roll,
                    "elevation": point.elevation,
                    "gps_bearing": point.gps_bearing,
                    "humidity": point.humidity,
                    "lumen": point.lumen,
                    "pressure": point.pressure,
                    "proximity": point.proximity,
                    "speed": point.speed,
                    "temperature": point.temperature,
                    "session_id": point.session_id,
                    "track_id": track_id,
                    "timestamp": point.timestamp,
                    "vehicle_type": point.vehicle_type.name,
                    "longitude": point.longitude,
                    "latitude": point.latitude,
                }
            )


def insert_segments(track_id: int, segments: FullSegmentData, owner: str,
                    db_cursor):
    segment_ids = []
    for segment, info, errors in segments:
        db_cursor.execute(
            get_query("insert-segment.sql"),
            {
                "track_id": track_id,
                "user_uuid": owner,
                "vehicle_type": info.vehicle_type.name,
                "geometry": info.geometry.ExportToWkb(),
                "start_date": info.start_date,
                "end_date": info.end_date,
            }
        )
        segment_ids.append(db_cursor.fetchone()[0])
    return segment_ids


def get_session_id(parsed_points: List[PointData]):
    return parsed_points[0].session_id


def process_points(points: List[PointData], **settings):
    validate_points(points)
    filtered_points = filter_point_data(
        points,
        accuracy_threshold=settings["points_accuracy_threshold"],
        position_threshold=settings["points_position_threshold"]
    )
    return filtered_points


def process_segments(points: List[PointData], db_cursor, **settings):
    generate_segments_partial = partial(
        generate_segments,
        minute_threshold=settings["segments_minute_threshold"],
        distance_thresholds=settings["segments_distance_thresholds"]
    )
    initial_segments = generate_segments_partial(points)
    logger.debug(
        "generated {} initial segments with number of points: {}".format(
            len(initial_segments), [len(s) for s in initial_segments])
    )
    final_points = []
    for segment in initial_segments:
        filtered_segment_points = filter_pairwise_segment_points(
            segment, settings["segments_pairwise_stddev_coeff"])
        final_points.extend(filtered_segment_points)
    if len(final_points) < 2:
        raise exceptions.NonRecoverableError(
            "cannot generate final segments, not enough points left")
    final_segments = generate_segments_partial(final_points)
    filtered_segments = apply_segment_filters(
        final_segments,
        temporal_lower_bound=settings["segments_temporal_lower_bound"],
        temporal_upper_bound=settings["segments_temporal_upper_bound"],
        db_cursor=db_cursor,
        small_segments_threshold=settings["segments_small_threshold"]
    )
    result = []
    for segment in filtered_segments:
        info = get_segment_info(segment)

        type_ = info.vehicle_type
        avg, max_ = settings["segments_speed_thresholds"].get(type_, (0, 0))
        validation_errors = validate_segment_info(
            info,
            average_speed=avg,
            max_speed=max_,
            length=settings["segments_length_thresholds"].get(type_, 0),
            duration=settings["segments_duration_thresholds"].get(type_, 0),
        )
        result.append((segment, info, validation_errors))
    return result


def process_data(points: List[PointData], cursor,
                 **settings) -> FullSegmentData:
    """Process the raw collected points into segments"""
    filtered_points = process_points(points, **settings)
    if len(filtered_points) < 2:
        raise exceptions.NonRecoverableError(
            "cannot generate segments, not enough points left")
    segments_data = process_segments(filtered_points, cursor, **settings)
    if len(segments_data) == 0:
        raise exceptions.NonRecoverableError("no segments could be generated")
    else:
        return segments_data


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
    result = ""
    with zipfile.ZipFile(input_buffer) as zip_handler:
        for member_name in zip_handler.namelist():
            result += zip_handler.read(member_name).decode(encoding)
    return result


def get_track_owner_internal_id(keycloak_uuid: str, db_cursor):
    db_cursor.execute(
        "SELECT user_id FROM bossoidc_keycloak WHERE \"UID\" = %s",
        (keycloak_uuid,)
    )
    try:
        return db_cursor.fetchone()[0]
    except TypeError:
        raise exceptions.NonRecoverableError(
            "Could not determine track owner internal ID")


def filter_point_data(points: List["PointData"], accuracy_threshold: float,
                      position_threshold: float) -> List["PointData"]:
    """Remove parsed points that have invalid data

    Track point collections must obey the following logic:

    - point accuracy must be higher than or equal to the input
      ``accuracy_threshold`` value;
    - it is not necessary to keep points that are too close to each other even
      if they are not consecutive in time - this is a safeguard against very
      noisy GPS data, whereby sometimes reported positions are repeated

    Note: We do not filter out points based on whether they are inside the
          spatial or temporal region of interest yet. This is only done further
          down in the validation pipeline, once the points have been gathered
          into segments. The reason for this delayed filtering is to preserve
          segment individualities.

    """

    # speedy = [pt for pt in points if pt.speed > 0]
    # not_speedy_count = len(points) - len(speedy)
    # logger.debug(f"Removed {not_speedy_count} points with bad speed")
    accurate = [pt for pt in points if pt.accuracy <= accuracy_threshold]
    not_accurate_count = len(points) - len(accurate)
    logger.debug(f"Removed {not_accurate_count} points with bad accuracy")
    result = remove_spatially_similar_points(accurate, position_threshold)
    spatially_similar_count = len(accurate) - len(result)
    logger.debug(
        "Removed {} points too close to one another".format(
            spatially_similar_count)
    )
    return result


def remove_spatially_similar_points(points: List[PointData],
                                    threshold:float) -> List[PointData]:
    """Discard points which are near each other in space and time

    The input ``points`` are assumed to be ordered by their timestamp
    """

    result = []
    for point in points:
        # iterate backwards through last 10 items - these are the ones
        # temporally closer to current point
        for other_point in result[-1:-11:-1]:
            position_delta = point.get_distance(other_point)
            if position_delta < threshold:
                break
        else:
            result.append(point)
    return result


def parse_point_raw_data(data: str) -> List[PointData]:
    """Parse input data into a list of ``PointData`` instances"""
    points = []
    for index, line in enumerate(data.splitlines()):
        if index > 0 and line != "":  # ignoring first line, it is file header
            try:
                point = PointData.from_raw_point(line)
            except (IndexError, ValueError):
                logger.exception("Could not parse line {}".format(index))
            else:
                points.append(point)
    points.sort(key=lambda pt: pt.timestamp)  # timestamps must be ascending
    return points


def validate_points(points: List[PointData]):
    """Make sure parsed points are valid"""
    session_ids = set(pt.session_id for pt in points)
    if len(points) == 0:
        raise exceptions.NonRecoverableError(
            "There are no valid points in input data")
    elif len(session_ids) != 1:
        raise exceptions.NonRecoverableError(
            "Multiple session identifiers present in input data")


def generate_segments(points: List[PointData], minute_threshold: int,
                      distance_thresholds: dict) -> SegmentData:
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
        last_distance = pt.get_distance(last_point)
        too_far_away = last_distance > distance_thresholds[pt.vehicle_type]

        if vehicle_changed:
            logger.debug(
                "point: {} last_point: {} - vehicle type changed, starting "
                "new segment...".format(pt, last_point)
            )
            start_new_segment = True
        elif too_much_time_passed:
            logger.debug(
                "point: {} last_point: {} minutes_passed: {} - too much time "
                "has passed, starting new segment...".format(
                    pt, last_point, minutes_passed)
            )
            start_new_segment = True
        elif too_far_away:
            logger.debug(
                "point: {} last_point: {} last_distance: {}  - too far away, "
                "starting new segment...".format(pt, last_point, last_distance)
            )
            start_new_segment = True
        else:
            start_new_segment = False
        if start_new_segment:
            segments.append([pt])  # start new segment
        else:
            segments[-1].append(pt)
    long_enough_segments = [seg for seg in segments if len(seg) > 2]
    return long_enough_segments


def filter_invalid_temporal_points(segments: SegmentData,
                                   lower_bound: dt.datetime,
                                   upper_bound: dt.datetime) -> SegmentData:

    def _check_temporal_bounds(point: PointData):
        ts = point.timestamp
        result = lower_bound <= ts <= upper_bound
        if not result:
            logger.debug(
                "Point is outside temporal bounds {} <= {} <= {}".format(
                    lower_bound, ts, upper_bound)
            )
        return result

    return _reconcile_segments(segments, test_func=_check_temporal_bounds)


def filter_points_outside_region_of_interest(segments: SegmentData,
                                             region_of_interest: ogr.Geometry):


    def _check_intersection(point: PointData):
        logger.debug(f"check_intersection called with {point}")
        intersects_roi = point_intersects_roi(point, region_of_interest)
        if not intersects_roi:
            logger.debug("Point is outside region of interest")
        return intersects_roi


    def _segment_point_invalid_region_of_interest(new_segment,
                                                  original_segment,
                                                  point,
                                                  point_index,
                                                  result):
        """If current point is outside region of interest we discard it and
        start a new segment for subsequent points. Additionally, we check if
        it is possible to create a new point at the intersection with the
        region of interest in place of the current one.

        """

        try:
            previous_point = original_segment[point_index-1]
        except IndexError:
            pass  # there is no previous point on the segment
        else:
            if _check_intersection(previous_point):
                sub_segment = [previous_point, point]
                intersection_point = get_segment_intersection_point(
                    sub_segment, region_of_interest)
                new_segment.append(intersection_point)
        # discard this point and start a new segment
        if len(new_segment) > 0:
            result.append(new_segment[:])
            new_segment.clear()
        try:
            next_point = original_segment[point_index+1]
        except IndexError:
            pass  # there is no next point on the segment
        else:
            if _check_intersection(next_point):
                sub_segment = [point, next_point]
                intersection_point = get_segment_intersection_point(
                    sub_segment, region_of_interest)
                if intersection_point is not None:
                    new_segment.append(intersection_point)

    return _reconcile_segments(
        segments,
        _check_intersection,
        point_invalid_func=_segment_point_invalid_region_of_interest
    )


def point_intersects_roi(point: PointData, region_of_interest: ogr.Geometry):
    for i in range(region_of_interest.GetGeometryCount()):
        sub_geom = region_of_interest.GetGeometryRef(i)
        intersects = sub_geom.Intersects(point.geometry)
        if intersects:
            result = True
            break
    else:
        result = False
    return result


def get_segment_intersection_point(segment: List[PointData],
                                   region_of_interest: ogr.Geometry):
    geographic_geom, projected_geom = get_segment_geometry(segment)
    boundary = region_of_interest.GetBoundary()
    intersection = geographic_geom.Intersection(boundary)
    # `intersection` might be single or multi, convert to multi to make uniform
    multi_intersection = ogr.ForceToMultiPoint(intersection)
    first_intersection_geom = multi_intersection.GetGeometryRef(0)
    if first_intersection_geom is None:
        result = None
    else:
        intersection_coords = first_intersection_geom.GetPoint()
        result = PointData(
            latitude=intersection_coords[1],
            longitude=intersection_coords[0],
            accuracy=segment[1].accuracy,
            speed=segment[1].speed,
            timestamp=segment[1].timestamp,
            vehicle_type=segment[1].vehicle_type,
            session_id=segment[1].session_id
        )
        # now adjust the timestamp
        new_segment = [segment[0], result]
        new_segment_projected_geom= get_segment_geometry(new_segment)[1]
        length_factor = (
                new_segment_projected_geom.Length() / projected_geom.Length())
        segment_duration = get_segment_duration(segment)
        new_segment_duration = segment_duration * length_factor
        result.timestamp = segment[0].timestamp + dt.timedelta(
            seconds=new_segment_duration)
    return result


def _segment_point_valid(new_segment: List, original_segment: List[PointData],
                         point: PointData, point_index: int, result: List):
    new_segment.append(point)


def _segment_point_invalid(new_segment: List,
                           original_segment: List[PointData],
                           point: PointData,
                           point_index: int,
                           result: List):
    # discard this point and start a new segment
    if len(new_segment) > 0:
        result.append(new_segment[:])
        new_segment.clear()


def _reconcile_segments(segments: SegmentData, test_func,
                        point_valid_func: Callable=_segment_point_valid,
                        point_invalid_func: Callable=_segment_point_invalid):
    result = []
    for segment in segments:
        new_segment = []
        for index, point in enumerate(segment):
            if test_func(point):
                point_valid_func(new_segment, segment, point, index, result)
            else:
                point_invalid_func(new_segment, segment, point, index, result)
        if len(new_segment) > 0:
            result.append(new_segment)
    return result


def filter_small_segments(segments: SegmentData,
                          threshold: int=1) -> SegmentData:
    logger.debug(
        "filtering out segments with less than {} points...".format(threshold))
    return [s for s in segments if len(s) > threshold]


def get_region_of_interest(db_cursor):
    db_cursor.execute(get_query("select-region-of-interest.sql"))
    wkb = bytes(db_cursor.fetchone()[0])
    geometry = ogr.CreateGeometryFromWkb(wkb)
    return geometry


def get_segment_duration(segment: List[PointData]):
    """Return the duration of the input segment, measured in seconds"""
    temporal_delta = segment[-1].timestamp - segment[0].timestamp
    duration_seconds = (
            temporal_delta.days * 24 * 60 * 60 +
            temporal_delta.seconds +
            temporal_delta.microseconds / (1000 * 1000)
    )
    return duration_seconds


def get_segment_geometry(
        segment: List[PointData]) -> Tuple[ogr.Geometry, ogr.Geometry]:
    """Return a tuple with both geographic and projected segment geometries"""
    geographic_geom = ogr.Geometry(ogr.wkbLineString)
    projected_geom = ogr.Geometry(ogr.wkbLineString)
    for point in segment:
        geographic_geom.AddPoint(point.longitude, point.latitude)
        projected_geom.AddPoint(point.x, point.y)
    return geographic_geom, projected_geom


def get_length(linestring_geom: ogr.Geometry, coordinate_transformer):
    cloned_geom = linestring_geom.Clone()
    cloned_geom.Transform(coordinate_transformer)
    return cloned_geom.Length()


def get_segment_speeds(segment):
    """Return the maximum and minimum speed of the segment

    Speed is calculated as the rate of change in position of adjacent points

    """

    max_speed = 0
    min_speed = 1000  # just some big initialization value
    for p1_index in range(len(segment) - 1):
        p2_index = p1_index + 1
        p1 = segment[p1_index]
        p2 = segment[p2_index]
        sub_segment = [p1, p2]
        sub_segment_duration = get_segment_duration(sub_segment)
        geographic_geom, projected_geom = get_segment_geometry(sub_segment)
        sub_segment_length = projected_geom.Length()
        average_speed = sub_segment_length / sub_segment_duration
        max_speed = max(max_speed, average_speed)
        min_speed = min(min_speed, average_speed)
    return max_speed, min_speed


def get_segment_info(segment: List[PointData]):
    duration = get_segment_duration(segment)
    geographic_geom, projected_geom = get_segment_geometry(segment)
    length = projected_geom.Length()
    average_speed = length / duration
    max_speed, min_speed = get_segment_speeds(segment)
    return SegmentInfo(
        geometry=geographic_geom,
        projected_geometry=projected_geom,
        start_date=segment[0].timestamp,
        end_date=segment[-1].timestamp,
        duration=duration,
        length=length,
        average_speed=average_speed,
        max_speed=max_speed,
        min_speed=min_speed,
        vehicle_type=segment[0].vehicle_type
    )


def validate_segment_info(info: SegmentInfo, average_speed, max_speed,
                          length, duration):
    relevant_vehicle_types = [
        VehicleType.foot,
        VehicleType.bike
    ]
    _errors = []
    if info.vehicle_type not in relevant_vehicle_types:
        return []
    if info.average_speed > average_speed:
        _errors.append((
            utils.ValidationError.segment_speed_too_high,
            "average_speed",
            info.average_speed
        ))
    if info.max_speed > max_speed:
        _errors.append((
            utils.ValidationError.segment_speed_too_high,
            "max_speed",
            info.max_speed,
        ))
    if info.length > length:
        _errors.append((
            utils.ValidationError.segment_length_too_big,
            "length",
            info.length,
        ))
    if info.duration > duration:
        _errors.append((
            utils.ValidationError.segment_duration_too_long,
            "duration",
            info.duration,
        ))
    return [
        {
            "msg": e[0].name,
            "variable": e[1],
            "value": e[2],
            "vehicle_type": info.vehicle_type.name
        } for e in _errors
    ]


def apply_segment_filters(segments: SegmentData,
                          temporal_lower_bound: dt.datetime,
                          temporal_upper_bound: dt.datetime,
                          db_cursor,
                          small_segments_threshold: int=1) -> SegmentData:
    """Filter out invalid segments and points according to various criteria"""
    region_of_interest = get_region_of_interest(db_cursor)
    filters = [
        (
            filter_invalid_temporal_points,
            None,
            {
                "lower_bound": temporal_lower_bound,
                "upper_bound": temporal_upper_bound
            }
        ),
        (
            filter_points_outside_region_of_interest,
            (region_of_interest,),
            None
        ),
        (
            filter_small_segments,
            None,
            {
                "threshold": small_segments_threshold
            }
        ),

    ]
    current_segments = segments
    for handler in filters:
        func, args, kwargs = handler
        current_segments = func(
            current_segments, *(args or []), **(kwargs or {}))
        if len(current_segments) == 0:
            break
    return current_segments


def filter_pairwise_segment_points(points, stddev_coeffs):
    """Filter out segment points based on anomalous speed

    Anomalies are detected by getting the mean speed and stddev for all points
    and then removing points that are above a speed threshold

    """

    speeds = []
    for p1, p2 in zip(points, points[1:]):
        info = get_segment_info([p1, p2])
        coeff = stddev_coeffs.get(info.vehicle_type, 1)
        speeds.append((p1, p2, info.average_speed))
    speeds_vector = np.array(speeds)
    mean_speed = speeds_vector[:, 2].mean()
    std_speed = speeds_vector[:, 2].std()
    max_speed = speeds_vector[:, 2].max()
    speed_threhsold = mean_speed + coeff * std_speed
    logger.debug(
        "mean: {:0.3f} std: {:0.3f} max: {:0.3f} "
        "speed_threshold: {:0.3f}".format(mean_speed, std_speed, max_speed,
                                          speed_threhsold)
    )
    valid_segments_selection = np.where(speeds_vector[:, 2] <= speed_threhsold)
    valid_p1s = speeds_vector[valid_segments_selection][:, 0]
    filtered_points = list(valid_p1s)
    if speeds_vector[-1, 2] <= speed_threhsold:  # checking last point
        filtered_points.append(speeds_vector[-1, 1])
    logger.debug(f"removed {len(points) - len(filtered_points)} points")
    return filtered_points


def is_track_valid(segments_data):
    validation_errors = [s[2] for s in segments_data]
    return all([len(s) == 0 for s in validation_errors])
