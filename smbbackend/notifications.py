#########################################################################
#
# Copyright 2018, GeoSolutions Sas.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.
#
#########################################################################

"""Utilities for sending notifications to cloud services"""

import json
import logging
from typing import List

import boto3
from pyfcm import FCMNotification

from .utils import MessageType

logger = logging.getLogger(__name__)


def publish_message_to_sns(topic_arn: str, message_type: MessageType,
                           **message_payload):
    sns_client = boto3.client("sns")
    payload = message_payload.copy()
    payload["message_type"] = message_type.name
    message = {
        "default": ", ".join(
            ["{}: {}".format(str(k), str(v)) for k, v in payload.items()]),
        "lambda": json.dumps(payload)
    }
    publish_response = sns_client.publish(
        TopicArn=topic_arn,
        Message=json.dumps(message),
        MessageStructure="json"
    )
    if publish_response["ResponseMetadata"]["HTTPStatusCode"] != 200:
        raise RuntimeError("Could not publish {!r} message to SNS: {}".format(
            message_type.name, publish_response))


def publish_message_to_fcm(fcm_push_service: FCMNotification,
                           device_registration_ids: List[str],
                           smb_message_type: MessageType,
                           data_payload: dict=None,
                           low_priority=False,
                           dry_run=False):
    original_payload = data_payload.copy() if data_payload is not None else {}
    payload = {str(k): str(v) for k, v in original_payload.items()}
    payload["message_name"] = smb_message_type.name
    logger.debug("inside publish_message_to_fcm: {}".format(locals()))
    result = fcm_push_service.notify_multiple_devices(
        registration_ids=device_registration_ids,
        data_message=payload,
        low_priority=low_priority,
        dry_run=dry_run,
        content_available=True,  # background notifications on iOS 10
        extra_kwargs={
            "mutable_content": True  # rich notifications on iOS 10
        }
    )
    return result
