import pytest
from aerology_influxdb_api.aerology_influxdb_client import InfluxDBHandler
import datetime
import pandas as pd


def test_query_lane_status():
    start_time = datetime.datetime.strptime('2023-02-01 05:00:00', '%Y-%m-%d %H:%M:%S')
    end_time = datetime.datetime.strptime('2023-02-01 13:00:00', '%Y-%m-%d %H:%M:%S')
    handler = InfluxDBHandler('config_files.json')
    results = handler.query_lane_status("EWR", start_time, end_time)
    airport_data = handler._get_airport_data('EWR')
    print(results)
    assert list(results.columns) == ['valid_time'] + airport_data['lane_names']
    assert str(min(results.valid_time)) == str(start_time)
    assert str(max(results.valid_time)) == str(end_time)


def test_query_capacity_for_slotting():


    handler = InfluxDBHandler('config_files.json')
    results = handler.query_capacity_predictions_for_slotting("EWR")

    assert results.shape[1] ==5