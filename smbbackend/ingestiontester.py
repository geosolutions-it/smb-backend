#########################################################################
#
# Copyright 2018, GeoSolutions Sas.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.
#
#########################################################################

import argparse
import concurrent.futures
from fnmatch import fnmatch
import io
import logging
import os
import pathlib
import tempfile
from typing import Optional
import zipfile

import boto3
import dateutil.parser
import pytz

from . import processor
from . import utils

logger = logging.getLogger(__name__)


processor.ENABLE_TRACK_VALIDATION = True


def main():
    parser = _get_parser()
    args = parser.parse_args()
    _setup_logging(logging.DEBUG if args.verbose else logging.INFO)
    download_to = pathlib.Path(args.target_directory).expanduser().resolve()
    download_to.mkdir(parents=True, exist_ok=True)
    if not args.no_download:
        s3 = boto3.resource("s3")
        bucket = s3.Bucket(args.bucket_name)
        logger.info("Downloading files from S3...")
        download_new_files(
            bucket,
            download_to,
            args.download_workers,
            args.exclude_pattern,
            args.pattern,
            args.modified_threshold
        )
    logger.info("Validating files...")
    wkt_dir = pathlib.Path(tempfile.mkdtemp()) if args.save_wkt else None
    total_files, files_with_errors = process_files(
        download_to,
        args.pattern,
        db_parameters={
            "host": args.db_host,
            "port": args.db_port,
            "db_name": args.db_name,
            "user": args.db_user,
            "password": args.db_password,
        },
        temp_dir=wkt_dir
    )
    logger.info(
        f"Files processed: {total_files} - with errors: {files_with_errors}")
    logger.info("Done!")


def process_files(base_dir: pathlib.Path, item_pattern, db_parameters,
                  temp_dir: pathlib.Path):
    processing_futures = {}
    with concurrent.futures.ProcessPoolExecutor() as executor:
        total_files = 0
        files_with_errors = 0
        for item in base_dir.iterdir():
            pattern_passes = item_pattern is None or fnmatch(
                item.name, item_pattern)
            if item.is_file() and pattern_passes:
                total_files += 1
                future = executor.submit(
                    process_file, item, db_parameters, temp_dir)
                processing_futures[future] = item
        for future in concurrent.futures.as_completed(processing_futures):
            file_path = str(processing_futures[future])
            try:
                validation_errors = future.result()
                is_valid = all([len(s) == 0 for s in validation_errors])
            except Exception:
                logger.exception(
                    msg=f"Could not process file {str(file_path)}")
                files_with_errors += 1
            else:
                logger.info(
                    f"path: {str(file_path)} errors: {validation_errors}")
                if not is_valid:
                    files_with_errors += 1
    return total_files, files_with_errors



def process_file(path: pathlib.Path, db_parameters,
                 wkt_dir: Optional[pathlib.Path]):
    logger.info(f"Processing file {str(path)}...")
    with path.open() as fh:
        raw_data = fh.read()
    points = processor.parse_point_raw_data(raw_data)
    db_connection = utils.get_db_connection(
        host=db_parameters["host"],
        port=db_parameters["port"],
        dbname=db_parameters["db_name"],
        user=db_parameters["user"],
        password=db_parameters["password"],
    )
    cursor = db_connection.cursor()
    segments_data = processor.process_data(
        points,
        cursor,
        raise_on_invalid_data=False,
        **processor.DATA_PROCESSING_PARAMETERS
    )
    for segment_data in segments_data:
        logger.debug(f"{segment_data[1].geometry.ExportToWkt()}")
        if wkt_dir is not None:
            _save_wkt_to_file(path, wkt_dir, segment_data[1])
    db_connection.close()
    validation_errors = [s[2] for s in segments_data]
    return validation_errors


def _save_wkt_to_file(path: pathlib.Path, destination_dir: pathlib.Path,
                      segment_info: processor.SegmentInfo):
    session_id = path.stem.rpartition("_")[-1]
    logger.debug(f"session_id: {session_id}")
    wkt_file_path = (
            destination_dir / f"{session_id}_{segment_info.vehicle_type.name}")
    logger.debug(f"wkt_file_path: {wkt_file_path}")
    with wkt_file_path.open("w") as fh:
        fh.write(segment_info.geometry.ExportToWkt())
    logger.info(f"Saved wkt to {wkt_file_path}")


def download_new_files(bucket, download_to, max_workers, exclude: str=None,
                       pattern: str=None, modified_since=None):
    download_futures = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers) as executor:
        for obj_summary in bucket.objects.all():
            modified = obj_summary.last_modified
            local_path = get_local_path(obj_summary.key, download_to)
            if exclude is not None and fnmatch(obj_summary.key, exclude):
                logger.debug(f"Ignoring object {obj_summary.key!r}...")
                continue
            elif modified_since is not None and modified < modified_since:
                logger.debug(f"Ignoring object {obj_summary.key!r}...")
                continue
            elif pattern is not None and not fnmatch(obj_summary.key, pattern):
                logger.debug(f"Ignoring object {obj_summary.key!r}...")
                continue
            elif local_path.exists():
                logger.debug(f"Object {obj_summary.key!r} is already present "
                             f"locally, ignoring...")
                continue
            download_future = executor.submit(
                get_session_data,
                bucket.name,
                obj_summary.key,
                local_path
            )
            download_futures[download_future] = obj_summary.key
        for future in concurrent.futures.as_completed(download_futures):
            key = download_futures[future]
            try:
                future.result()
            except Exception:
                logger.exception(msg="Couldn't download {}".format(key))


def get_local_path(object_key: str, base_dir: pathlib.Path):
    local_name = object_key.replace("/", "__").rpartition(".")[0]
    return base_dir / f"{local_name}.csv"


def get_session_data(bucket_name, object_key: str, target_path: pathlib.Path,
                     use_boto_internal_threads=False):
    logger.info(f"Downloading and extracting {object_key!r} to "
                f"{str(target_path)!r}...")
    # boto3 docs recommend creating new resources for each thread
    # https://boto3.amazonaws.com/v1/documentation/api/latest/guide/
    #     resources.html#multithreading-multiprocessing
    s3 = boto3.resource("s3")
    bucket = s3.Bucket(bucket_name)
    config = boto3.s3.transfer.TransferConfig(
        use_threads=use_boto_internal_threads)
    input_buffer = io.BytesIO()
    bucket.download_fileobj(
        Key=object_key,
        Fileobj=input_buffer,
        Config=config
    )
    with zipfile.ZipFile(input_buffer) as zip_handler:
        for member_name in zip_handler.namelist():
            with target_path.open("wb") as fh:
                fh.write(zip_handler.read(member_name))


def _get_parser():


    def _get_modified_threshold(str_value):
        return dateutil.parser.parse(str_value).replace(tzinfo=pytz.utc)


    parser = argparse.ArgumentParser()
    parser.add_argument(
        "target_directory",
        help="Base directory where files will be downloaded to. It will be "
             "created if needed",
    )
    parser.add_argument(
        "-b",
        "--bucket-name",
        help="Name of the S3 bucket to use. Defaults to the value of the "
             "`S3_BUCKET` environment variable (currently '%(default)s'), if "
             "any",
    )
    parser.add_argument(
        "-x",
        "--no-download",
        help="Do not download any new data, use only what is already "
             "available locally",
        action="store_true",
    )
    parser.add_argument(
        "-m",
        "--modified-threshold",
        help="Earlier date that should be considered. All objects older than "
             "this will be ignored. This should be a string parseable with "
             "`dateutil.parser` and is expected to be in UTC time.",
        type=_get_modified_threshold
    )
    parser.add_argument(
        "-w",
        "--download-workers",
        default=8,
        type=int,
        help="Maximum concurrent download threads",
    )
    parser.add_argument(
        "-e",
        "--exclude-pattern",
        help="An fnmatch pattern that is matched against existing object keys "
             "to filter out objects that should not be downloaded"
    )
    parser.add_argument(
        "-p",
        "--pattern",
        help="An fnmatch pattern that is matched against both S3 objects and "
             "existing local files and used to determine which files get "
             "downloaded and processed"
    )
    parser.add_argument(
        "-s",
        "--save-wkt",
        action="store_true",
        help="Whether files with WKT for each segment should be saved"
    )
    parser.add_argument(
        "--verbose",
        action="store_true"
    )
    parser.add_argument("--db-host")
    parser.add_argument("--db-port", type=int)
    parser.add_argument("--db-name")
    parser.add_argument("--db-user")
    parser.add_argument("--db-password")
    parser.set_defaults(
        bucket_name=os.getenv("S3_BUCKET"),
        db_host=os.getenv("DB_HOST", "localhost"),
        db_port=int(os.getenv("DB_PORT", "5432")),
        db_name=os.getenv("DB_NAME"),
        db_user=os.getenv("DB_USER"),
        db_password=os.getenv("DB_PASSWORD"),
    )
    return parser


def _setup_logging(level):
    logging.basicConfig(level=level)
    to_disable = [
        "botocore",
    ]
    for log_name in to_disable:
        logging.getLogger(log_name).propagate = False
