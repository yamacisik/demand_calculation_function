import logging
import os
from datetime import datetime

import pandas as pd
import boto3
from aerology_influxdb_api.aerology_influxdb_client import InfluxDBHandler
from botocore.config import Config
from boto3.dynamodb.conditions import Key

from dependencies.utils.flight_msg_utils import calculate_demand_from_flights
from dependencies.utils.dynamodb_utils import query_active_flights, format_time_values,clean_active_flights

# db access constants:
LOGGING_LEVEL = logging.INFO  # default logging level
DB_TABLE = os.environ.get('DYNAMODB_TABLE')  # table to use, created by the sam template
DYNAMODB_CONFIG = Config(connect_timeout=5, read_timeout=10, retries={'max_attempts': 10})  # config for dynamoDB conn

# globals
_logger = logging.getLogger()  # logger handle
_logger.setLevel(LOGGING_LEVEL)
_influxdb_client = InfluxDBHandler('./res/config.json')  # influxdb handle

_dynamodb_resource = boto3.resource('dynamodb', config=DYNAMODB_CONFIG)
_db_table = _dynamodb_resource.Table(DB_TABLE)


def lambda_demand_calculator(event, context):
    """lambda function that on a schedule processes capacity messages from s3 into dynamoDB

    Parameters
    ----------
    event: dict, required
        API Gateway Lambda Proxy Input Format
        Event doc: https://docs.aws.amazon.com/apigateway/latest/developerguide/set-up-lambda-proxy-integrations.html#api-gateway-simple-proxy-for-lambda-input-format

    context: object, required
        Lambda Context runtime methods and attributes
        Context doc: https://docs.aws.amazon.com/lambda/latest/dg/python-context-object.html

    Returns
    ------
    API Gateway Lambda Proxy Output Format: dict
        Return doc: https://docs.aws.amazon.com/apigateway/latest/developerguide/set-up-lambda-proxy-integrations.html
    """
    _ = event, context

    _logger.info(f"querying flights ...")
    active_flights = query_active_flights('ActiveFlightsStaging', 'EWR')
    _logger.info(f"loaded {len(active_flights)} flights.")

    _logger.info(f"Cleaning active flights.")
    active_flights = clean_active_flights(active_flights)

    demand = calculate_demand_from_flights(active_flights)

    _logger.info(f"pushing results to influxDB..")
    _influxdb_client.push_demand(data = demand, airport = 'EWR')
    _logger.info(f"successfully pushed results to influxDB.")

    return {"statusCode": 200}


