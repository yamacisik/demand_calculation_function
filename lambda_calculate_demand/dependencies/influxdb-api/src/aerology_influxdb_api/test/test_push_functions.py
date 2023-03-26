import pandas as pd
import pytest
from src.aerology_influxdb_api.aerology_influxdb_client import InfluxDBHandler
import datetime


def test_pushing_flight_predictions():
    current_time = datetime.datetime.utcnow().replace(minute=0, second=0, microsecond=0)

    data = [
        {
            "_time": current_time,

            "predicted_delay": 100,
            "published_delay": 250,

            "eta_hour": "2022-03-01T00:00:00Z",
            "slt_hour": "2022-03-01T01:00:00Z",
            "flight_id": "ABC123"}, {

            "_time": current_time,

            "predicted_delay": 120,
            "published_delay": 340,

            "eta_hour": "2022-03-01T00:00:00Z",
            "slt_hour": "2022-03-01T01:00:00Z",
            "flight_id": "ABC123"}
    ]

    handler = InfluxDBHandler('config.json')
    handler.push_flight_calculations(data, 'EWR')

    assert True


def test_push_demand():

    data = {'valid_time':["2023-03-24 21:00:00+00:00","2023-03-24 22:00:00+00:00",
                          "2023-03-24 23:00:00+00:00","2023-03-25 00:00:00+00:00"],'demand':[3,5,10,24]}
    data = pd.DataFrame(data)
    handler = InfluxDBHandler('config.json')
    handler.push_demand(data, 'EWR')

    assert True


