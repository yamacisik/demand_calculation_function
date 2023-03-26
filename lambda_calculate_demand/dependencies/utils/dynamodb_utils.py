import boto3
from boto3.dynamodb.conditions import Key
import pandas as pd
import logging
from datetime import datetime,timezone,timedelta
cancel_triggers=  ["FD_FLIGHT_CANCEL_MSG, UPDATE_CANCEL_TIMEOUT, UPDATE_INTERNATIONAL_CANCEL_TIMEOUT, TMI_UPDATE"],

def query_active_flights(table_name='ActiveFlights', airport='EWR') -> pd.DataFrame:
    """
    Queries a DynamoDB table to retrieve current active flights for a specified airport.

    Parameters
    ----------
    table_name : str
        The name of the DynamoDB table to query.

    airport : str
        The name of the airport for which to retrieve active flights.

    Returns
    -------
    pd.DataFrame
        A dataframe containing the active flights for the specified airport. The dataframe has one row for each active
        flight, with columns for flight information such as the flight ID, departure time, arrival time, and current status.
        If there are no active flights for the specified airport, the dataframe will be empty.

    """

    dynamodb = boto3.resource("dynamodb", region_name="us-east-1")

    table = dynamodb.Table(table_name)

    response = table.scan(FilterExpression=Key('airport').eq(airport))

    items = response['Items']
    active_flights_df = pd.DataFrame(items)

    # read the remaining items in the table, since the scan() method only returns up to 1 MB of data at a time
    while 'LastEvaluatedKey' in response:
        response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
        items = response['Items']
        active_flights_df = active_flights_df.append(pd.DataFrame(items))

    logging.info(f"Retrieved {len(active_flights_df)} active flights for {airport} from DynamoDB table {table_name}.")
    return active_flights_df


def clean_active_flights(active_flights_df):
    """
    Cleans the active flights data for flights that don't represent demand.
    Drops cancelled and stale flights.

    Parameters
    ----------

    config_path
    active_flights_df: pd.DataFrame
        The active flights data to be cleaned.
    activeFlights_lag: int
    airport:str
    The airport for which to clean the active flights data.


    Returns
    -------
    pd.DataFrame
        The cleaned active flights data.
    """

    active_flights_df = format_time_values(active_flights_df)
    operating_activeFlights_df = drop_cancelled_flights(active_flights_df, cancel_triggers)
    inbound_flights_df = drop_stale_flights(operating_activeFlights_df)

    return inbound_flights_df


def format_time_values(active_flights_df: pd.DataFrame) -> pd.DataFrame:
    """
    Formats and sorts a dataframe of active flights.

    This function converts Unix timestamps in the specified columns to pandas.Timestamp objects,
    and sorts the dataframe by estimated arrival time.

    Parameters
    ----------
    active_flights_df : pd.DataFrame
        A pandas dataframe of active flights.

    Returns
    -------
    pd.DataFrame
        The formatted dataframe of active flights.
    """

    timestamp_cols = ['sched_dept_time',
                      'est_dept_time',
                      'sched_landing_time',
                      'last_msg_time',
                      'flight_creation_time',
                      'est_arrival_time'
                      ]
    convert_unix_timestamps_to_datetime(active_flights_df, timestamp_cols)

    # sort by estimated arrival time
    active_flights_df = active_flights_df.sort_values(by='est_arrival_time')

    return active_flights_df


def drop_cancelled_flights(active_flights_df, cancel_triggers):
    """
    Drops flights that have canceled because they no longer demand a slot.

    Not all 'flightPlanCancellation' msgTypes actually cancel the flight;
    need to interpret fdTrigger (e.g. flight might have an alternate route
    planned that requires second flight plan — once airline determines that
    the alt route is not required, they'll cancel the alt flight plan and
    the flight still operates).

    Returns dataframe of operating (i.e. not canceled) flights
    """
    # Define the cancelled flights mask

    cancel_mask = active_flights_df['msg_trigger'].isin(cancel_triggers)

    # Log the number of cancelled flights and drop them
    cncl_flights_ct = cancel_mask.sum()
    logging.info(f"{cncl_flights_ct} canceled flight(s) dropped..")

    # Return the cleaned DataFrame
    return active_flights_df.loc[~cancel_mask]


def drop_stale_flights(operating_activeFlights_df: pd.DataFrame):
    """
    Drops non-sensical flights.

    For example, flight has not yet departed its origin (we know flight
    not departed because etdType != 'ACTUAL') but estimated to arrive in the past.

    Returns dataframe of still-inbound flights
    """
    # Define the stale flights mask
    stale_etd_types = ['SCHEDULED', 'PROPOSED', None]
    stale_mask = (
            (operating_activeFlights_df['est_dept_time_type'].isin(stale_etd_types)) &
            (operating_activeFlights_df['est_arrival_time'] < datetime.now(timezone.utc)))

    stale_flights_ct = stale_mask.sum()
    logging.info(f"{stale_flights_ct} stale flight(s) dropped..")

    return operating_activeFlights_df.loc[~stale_mask]


def convert_unix_timestamps_to_datetime(df, columns):
    for col in columns:
        # Dynamo Unix integer read as string
        if df[col].dtype == 'O':
            df[col] = pd.to_numeric(df[col], errors='coerce')
        df[col] = pd.to_datetime(df[col], unit='s', errors='coerce')
        df[col] = df[col].dt.tz_localize('UTC')
