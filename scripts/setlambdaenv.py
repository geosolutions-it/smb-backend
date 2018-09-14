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
import logging
import os

import boto3


logger = logging.getLogger(__name__)


def main(lambda_name):
    lambda_client = boto3.client("lambda")
    # this is just for validation that the lambda exists
    lambda_client.get_function_configuration(FunctionName=lambda_name)
    relevant_env = get_relevant_env_variables(lambda_name)
    logger.debug("relevant_env: {}".format(relevant_env))
    update_response = update_lambda_environment_variables(
        lambda_name, lambda_client, **relevant_env)
    logger.info(update_response)


def get_relevant_env_variables(function_name, environment=os.environ) -> dict:
    prefix = "ZAPPA_{}_".format(function_name.replace("-", "_").upper())
    result = {}
    for name, value in environment.items():
        if name.startswith(prefix):
            edited_name = name.replace(prefix, "")
            result[edited_name] = value
    return result


def update_lambda_environment_variables(lambda_name, lambda_client,
                                        **environment_variables):
    response = lambda_client.update_function_configuration(
        FunctionName=lambda_name,
        Environment={
            "Variables": environment_variables
        }
    )
    return response


def get_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "lambda_name",
    )
    parser.add_argument(
        "--verbose",
        action="store_true"
    )
    return parser


def configure_logging(level, disable_loggers=None):
    to_disable = list(disable_loggers) if disable_loggers is not None else []
    logging.basicConfig(level=level)
    for name in to_disable:
        logging.getLogger(name).propagate = False


if __name__ == "__main__":
    parser = get_parser()
    args = parser.parse_args()
    configure_logging(
        level=logging.DEBUG if args.verbose else logging.INFO,
        disable_loggers=["botocore", "urllib3"]
    )
    main(args.lambda_name)
