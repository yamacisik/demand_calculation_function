from datetime import datetime, timedelta
import pandas as pd

AIRPORT = 'EWR'
DEFAULT_TIME_STAMP = '1975-01-01T01:01:00Z'
CALC_HORIZON_HOURS = 20  # this many hours in future to calculate capacity
RUNTIME_OFFSET_HOURS = 1  # assume validty of the messages for the past hour (since we check past N hour messages)


def _string_to_datetime(time_string):
    """
    converts tfms time string to datetime object
    Parameters
    ----------
    time_string: str, has the form 'YYYY-MM-DD:HH:MM:SSZ' or 'YYYY-MM-DD:HH:MM:SS.msZ'

    Returns datetime
    -------

    """
    return datetime.strptime(time_string.split('.')[0].split('Z')[0], '%Y-%m-%dT%H:%M:%S')


def _get_current_system_time(runtime_offset_hours):
    """
    get the system time to check the messages against.
    Returns
    -------
    """
    return datetime.now().replace(minute=0, second=0, microsecond=0) - timedelta(hours=runtime_offset_hours)


def calculate_demand_from_flights(active_flights):

    active_flights['scheduled_landing_hour']=active_flights.sched_landing_time.dt.floor('H')
    demand_by_hour = active_flights.groupby('scheduled_landing_hour').count()['sched_landing_time'].\
        reset_index()

    demand_by_hour.columns = ['valid_time', 'demand']
    return demand_by_hour

