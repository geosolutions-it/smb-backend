#########################################################################
#
# Copyright 2018, GeoSolutions Sas.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.
#
#########################################################################

import json

import psycopg2


def extract_sns_message(event: dict) -> dict:
    try:
        raw_sns_message = event["Records"][0]["Sns"]["Message"]
    except (KeyError, IndexError):
        raise RuntimeError("Unsupported event")
    else:
        return json.loads(raw_sns_message)


def get_db_connection(dbname, user, password, host="localhost", port="5432"):
    return psycopg2.connect(
        host=host,
        port=port,
        dbname=dbname,
        user=user,
        password=password
    )


def publish_message(message: str, topic_arn: str, session):
    sns_client = session.client("sns")
    publish_response = sns_client.publish(
        TopicArn=topic_arn,
        Message=message,
    )
    if publish_response["ResponseMetadata"]["HTTPStatusCode"] != 200:
        raise RuntimeError(
            "Could not publish `track created` message to "
            "SNS: {}".format(publish_response)
        )
