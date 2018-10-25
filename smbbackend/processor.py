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
import io
import logging
import re
from typing import List
from typing import Tuple
from typing import Callable
import zipfile

import boto3
from osgeo import ogr
from osgeo import osr
import pytz

from ._constants import VehicleType
from . import exceptions
from . import utils
from .utils import get_query

logger = logging.getLogger(__name__)


DATA_PROCESSING_PARAMETERS = {
    "segments_distance_thresholds": {  # in m
        VehicleType.foot: 100,
        VehicleType.bike: 300,
        VehicleType.motorcycle: 500,
        VehicleType.car: 500,
        VehicleType.bus: 500,
        VehicleType.train: 1000,
    },
    "segments_minute_threshold": 20,
    "segments_temporal_lower_bound": dt.datetime(2018, 1, 1, tzinfo=pytz.utc),
    "segments_temporal_upper_bound": (
        dt.datetime.now(pytz.utc) + dt.timedelta(days=1)),
    "segments_small_threshold": 1,
    "points_position_threshold": 0.1,
    "points_accuracy_threshold": 10,
    "segments_speed_thresholds": {  # (average_speed, max_speed), in m/s
        VehicleType.foot: (2.78, 2.78),  # 10km/h , 10 km/h
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
}

SegmentData = List[List["PointData"]]
FullSegmentData = List[Tuple[List["PointData"], "SegmentInfo"]]

SegmentInfo = namedtuple("SegmentInfo", [
    "geometry",
    "start_date",
    "end_date",
    "duration",  # measured in seconds
    "length",  # in m
    "average_speed",  # measured in m/s
    "max_speed",  # measured in m/s
    "min_speed",  # measured in m/s
    "vehicle_type"
])


class PointData(object):

    geometry: ogr.Geometry = None
    session_id: int = 0
    timestamp: dt.datetime = None
    vehicle_type: VehicleType = None

    def __init__(self, latitude: float, longitude: float, accuracy: float,
                 timestamp: dt.datetime, vehicle_type: VehicleType,
                 session_id: int):
        self.timestamp = timestamp
        self.vehicle_type = vehicle_type
        self.session_id = session_id
        self.accuracy = accuracy
        self.geometry = ogr.Geometry(ogr.wkbPoint)
        self.geometry.AddPoint(longitude, latitude)

    def __str__(self):
        return (
            "Point(latitude={}, longitude={}, accuracy={}, timestamp={}, session_id={}, "
            "vehicle_type={})".format(
                self.latitude,
                self.longitude,
                self.accuracy,
                self.timestamp.isoformat(),
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
            latitude=float(info[12]),
            longitude=float(info[13]),
            accuracy=float(info[3]),
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

    def get_distance(self, other_point: "PointData",
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


def ingest_s3_data(s3_bucket_name: str, object_key: str,
                   db_cursor) -> Tuple[int, List[dict]]:
    """Ingest track data into smb database"""
    try:
        track_owner = get_track_owner_uuid(object_key)
    except AttributeError:
        raise exceptions.NonRecoverableError(
            "Could not determine track owner for object {}".format(object_key))
    logger.debug("Retrieving data from S3 bucket...")
    raw_data = get_data_from_s3(s3_bucket_name, object_key)
    points = parse_point_raw_data(raw_data)
    session_id = get_session_id(points)
    segments, validation_errors = process_data(
        points,
        db_cursor,
        raise_on_invalid_data=False,
        **DATA_PROCESSING_PARAMETERS
    )
    track_id = save_track(
        session_id, segments, track_owner, validation_errors, db_cursor)
    utils.update_track_info(track_id, db_cursor)
    return track_id, validation_errors


def save_track(session_id, segments: FullSegmentData, owner: str,
               validation_errors: List[dict], db_cursor):
    owner_internal_id = get_track_owner_internal_id(owner, db_cursor)
    track_id = insert_track(session_id, owner_internal_id, validation_errors,
                            db_cursor)
    insert_points(track_id, segments, db_cursor)
    insert_segments(track_id, segments, owner, db_cursor)
    return track_id


def insert_track(session_id: int, owner: int, validation_errors: List[dict],
                 db_cursor) -> int:
    """Insert track data into the main database"""
    query = get_query("insert-track.sql")
    db_cursor.execute(
        query,
        {
            "owner_id": owner,
            "session_id": session_id,
            "created_at": dt.datetime.now(pytz.utc),
            "is_valid": True if len(validation_errors) == 0 else False,
            "validation_error": ", ".join(
                error["message"] for error in validation_errors)
        }
    )
    track_id = db_cursor.fetchone()[0]
    return track_id


def insert_points(track_id: int, segments: FullSegmentData, db_cursor):
    query = get_query("insert-point.sql")
    for segment, info in segments:
        for point in segment:
            db_cursor.execute(
                query,
                {
                    "vehicle_type": point.vehicle_type.name,
                    "track_id": track_id,
                    "longitude": point.longitude,
                    "latitude": point.latitude,
                    "sessionid": point.session_id,
                    "timestamp": point.timestamp,
                }
            )


def insert_segments(track_id: int, segments: FullSegmentData, owner: str,
                    db_cursor):
    segment_ids = []
    for segment, info in segments:
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


def process_data(points: List[PointData], db_cursor,
                 raise_on_invalid_data=True,
                 **settings) -> Tuple[FullSegmentData, List]:
    """Process the raw collected points into segments

    The following operations are performed on the raw data:

    - Parsing into ``PointData`` instances
    - Verifying that enough points exist
    - Removing points that have invalid timestamps
    - Removing points that are too close to one another
    - Grouping consecutive points into segments, based on similarities
    - Removing invalid points from segments and make segments consistent

    """

    errors = []
    validate_points(points)
    coordinate_transformer = get_coordinate_transformer()
    filtered_points = filter_point_data(
        points,
        coordinate_transformer,
        accuracy_threshold=settings["points_accuracy_threshold"],
        position_threshold=settings["points_position_threshold"]
    )
    segments = generate_segments(
        filtered_points,
        coordinate_transformer,
        minute_threshold=settings["segments_minute_threshold"],
        distance_thresholds=settings["segments_distance_thresholds"]
    )
    filtered_segments = apply_segment_filters(
        segments,
        temporal_lower_bound=settings["segments_temporal_lower_bound"],
        temporal_upper_bound=settings["segments_temporal_upper_bound"],
        db_cursor=db_cursor,
        small_segments_threshold=settings["segments_small_threshold"]
    )
    try:
        validate_segments(
            filtered_segments, coordinate_transformer, **settings)
    except exceptions.RecoverableError as exc:
        logger.exception("Segment validation failed")
        errors.append({
            "message": exc.args[0],
            "variable": exc.args[1],
            "value": exc.args[2],
            "vehicle_type": exc.args[3],
        })
        if raise_on_invalid_data:
            raise
    result = []
    for segment in filtered_segments:
        info = get_segment_info(segment, coordinate_transformer)
        result.append((segment, info))
    return result, errors


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


def get_track_owner_uuid(object_key: str) -> str:
    search_obj = re.search(r"[\w-]{36}", object_key)
    return search_obj.group()


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


def filter_point_data(points: List["PointData"], coordinate_transformer,
                      accuracy_threshold: float,
                      position_threshold: float) -> List["PointData"]:
    """Remove parsed points that have invalid data

    Track point collections must obey the following logic:

    - point accuracy must be higher than or equal to the input
      ``accuracy_threshold`` value;
    - timestamps are always ascending between consecutive points;
    - it is not necessary to keep consecutive points that are too close in
      space to each other

    Note: We do not filter out points based on whether they are inside the
          spatial or temporal region of interest yet. This is only done further
          down in the validation pipeline, once the points have been gathered
          into segments. The reason for this delayed filtering is to preserve
          segment individualities.

    """

    result = []
    if points[0].accuracy <= accuracy_threshold:
        result.append(points[0])
    for p1_index in range(len(points) - 1):
        p1 = points[p1_index]
        p2 = points[p1_index+1]
        position_delta = p2.get_distance(p1, coordinate_transformer)
        if p2.timestamp < p1.timestamp:
            logger.debug("Removing consecutive point with invalid timestamp")
        elif position_delta < position_threshold:
            logger.debug(
                "Removing consecutive point too close to the previous one")
        elif p2.accuracy > accuracy_threshold:
            logger.debug(
                "Removing point with bad accuracy ({} m)".format(p2.accuracy))
        else:
            result.append(p2)
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


def generate_segments(points: List[PointData], transformer,
                      minute_threshold: int,
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
        last_distance = pt.get_distance(last_point, transformer)
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
    return segments


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
        result = region_of_interest.Intersects(point.geometry)
        if not result:
            logger.debug("Point is outside region of interest")
        return result


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
                new_segment.append(intersection_point)

    return _reconcile_segments(
        segments,
        _check_intersection,
        point_invalid_func=_segment_point_invalid_region_of_interest
    )


def get_segment_intersection_point(segment: List[PointData],
                                   region_of_interest: ogr.Geometry):
    geom = get_segment_geometry(segment)
    boundary = region_of_interest.GetBoundary()
    intersection = geom.Intersection(boundary)
    # `intersection` might be single or multi, convert to multi to make uniform
    multi_intersection = ogr.ForceToMultiPoint(intersection)
    first_intersection_geom = multi_intersection.GetGeometryRef(0)
    intersection_coords = first_intersection_geom.GetPoint()
    result = PointData(
        latitude=intersection_coords[1],
        longitude=intersection_coords[0],
        timestamp=segment[1].timestamp,
        vehicle_type=segment[1].vehicle_type,
        session_id=segment[1].session_id
    )
    # now adjust the timestamp
    new_segment = [segment[0], result]
    new_segment_geometry = get_segment_geometry(new_segment)
    length_factor = new_segment_geometry.Length() / geom.Length()
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
    min_speed = 1000  # just some big initialization value
    for p1_index in range(len(segment) - 1):
        p2_index = p1_index + 1
        p1 = segment[p1_index]
        p2 = segment[p2_index]
        sub_segment = [p1, p2]
        sub_segment_duration = get_segment_duration(sub_segment)
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
    average_speed = length / duration
    max_speed, min_speed = get_segment_speeds(segment, coordinate_transformer)
    return SegmentInfo(
        geometry=segment_geometry,
        start_date=segment[0].timestamp,
        end_date=segment[-1].timestamp,
        duration=duration,
        length=length,
        average_speed=average_speed,
        max_speed=max_speed,
        min_speed=min_speed,
        vehicle_type=segment[0].vehicle_type
    )


def validate_segment(segment: List[PointData], info: SegmentInfo, **settings):
    relevant_vehicle_types = [
        VehicleType.foot,
        VehicleType.bike
    ]
    if info.vehicle_type in relevant_vehicle_types:
        validate_segment_speed(info, settings["segments_speed_thresholds"])
        validate_segment_length(info, settings["segments_length_thresholds"])
        validate_segment_duration(
            info, settings["segments_duration_thresholds"])


def validate_segment_speed(segment_info: SegmentInfo, speed_thresholds):
    avg_threshold, max_threshold = speed_thresholds.get(
        segment_info.vehicle_type, (0, 0))
    if segment_info.average_speed > avg_threshold:
        raise exceptions.RecoverableError(
            utils.ValidationError.segment_speed_too_high.name,
            "average_speed",
            segment_info.average_speed,
            segment_info.vehicle_type.name
        )
    if segment_info.max_speed > max_threshold:
        raise exceptions.RecoverableError(
            utils.ValidationError.segment_speed_too_high.name,
            "max_speed",
            segment_info.max_speed,
            segment_info.vehicle_type.name
        )


def validate_segment_length(segment_info: SegmentInfo, length_thresholds):
    length_threshold = length_thresholds.get(segment_info.vehicle_type, 0)
    if segment_info.length > length_threshold:
        raise exceptions.RecoverableError(
            utils.ValidationError.segment_length_too_big.name,
            "length",
            segment_info.length,
            segment_info.vehicle_type.name
        )


def validate_segment_duration(segment_info: SegmentInfo, duration_thresholds):
    duration_threshold = duration_thresholds.get(segment_info.vehicle_type, 0)
    if segment_info.duration > duration_threshold:
        raise exceptions.RecoverableError(
            utils.ValidationError.segment_duration_too_long,
            "duration",
            segment_info.duration,
            segment_info.vehicle_type.name
        )


def validate_segments(segments: SegmentData, coordinate_transformer,
                      **settings):
    if len(segments) == 0:
        raise exceptions.NonRecoverableError("No valid segments")
    else:
        for segment in segments:
            info = get_segment_info(segment, coordinate_transformer)
            validate_segment(segment, info, **settings)


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
