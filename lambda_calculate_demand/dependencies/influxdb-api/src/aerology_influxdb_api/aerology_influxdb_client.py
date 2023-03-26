import json
import warnings
from datetime import datetime, timedelta
import time
import os
import influxdb_client
import pandas
from jsonschema import validate
from .schemas import configuration_json_schema
from influxdb_client import InfluxDBClient
from influxdb_client import Point
from influxdb_client.client.write_api import SYNCHRONOUS
from influxdb_client.domain.write_precision import WritePrecision

measurement_vector_cap = 'gdp_apt_capacity'
measurement_real_cap = "actual_capacity"
measurement_cap_pred = "capacity_prediction"
measurement_lanes = "lane_status"
measurement_demand = "demand"
delay_calculations = "delay_calculations"


class InfluxDBHandler:

    def push_capacity_predictions(self, data: pandas.DataFrame, airport: str):
        """expects an input with columns:
        'index'
        'predicted_time',
        'predicted_capacity',
        'prediction_variance',
        'prediction_offset' """

        airport_data = self._get_airport_data(airport)
        if airport_data is None:
            warnings.warn("can not push data, no settings are configured for the selected airport!")
            return

        for index, row in data.iterrows():
            # dict_structure = {"measurement": 'capacity_prediction',
            #                   "tags": {"init_time": row['init_time'], "model_id": row['model_id']},
            #                   "fields": {"predicted_capacity": row['predicted_capacity'],
            #                              "prediction_variance": row['prediction_variance'],
            #                              "time": row['predicted_time']}}
            # data_point = Point.from_dict(dict_structure, WritePrecision.S)

            data_point = influxdb_client.Point(measurement_cap_pred) \
                .tag('init_time', row['init_time']) \
                .tag('model_id', row['model_id']) \
                .field('predicted_capacity', row['predicted_capacity']) \
                .field('prediction_variance', row['prediction_variance']) \
                .time(int(row['predicted_time'].timestamp()), WritePrecision.S)

            # TODO: verify that it doesn't need unix timestamp
            self._influx_writer.write(bucket=airport_data['influx_bucket'], org=self._config['influx']['org'],
                                      record=data_point)
        self._influx_writer.flush()

    def push_capacity_measurements(self, data: pandas.DataFrame, airport: str):
        airport_data = self._get_airport_data(airport)
        if airport_data is None:
            warnings.warn("can not push data, no settings are configured for the selected airport!")
            return

        for index, row in data.iterrows():
            data_point = influxdb_client.Point(measurement_real_cap) \
                .field('measured_capacity', row['capacity']) \
                .time(row['time'], WritePrecision.S)  # TODO: verify that it doesn't need unix timestamp
            self._influx_writer.write(bucket=airport_data['influx_bucket'], org=self._config['influx']['org'],
                                      record=data_point)
        self._influx_writer.flush()

    def push_vectoral_capacity_measurements(self, data: pandas.DataFrame, init_time: datetime, airport: str,
                                            msg_type: str):
        airport_data = self._get_airport_data(airport)
        if airport_data is None:
            warnings.warn("can not push data, no settings are configured for the selected airport!")
            return

        # create vector string:
        start_time = data.iloc[0]['time'].strftime("%d/%m/%y %H:%M")
        end_time = data.iloc[-1]['time'].strftime("%d/%m/%y %H:%M")
        vector_field_str = f"time: {start_time} - {end_time}, values: "
        for _, row in data.iterrows():
            vector_field_str += f"{row['capacity']},"

        data_point = influxdb_client.Point(measurement_vector_cap) \
            .field('capacity_vector', vector_field_str) \
            .tag('message_type', msg_type) \
            .time(init_time, WritePrecision.S)

        self._influx_writer.write(bucket=airport_data['influx_bucket'], org=self._config['influx']['org'],
                                  record=data_point)
        self._influx_writer.flush()

    def push_lane_status(self, data: pandas.DataFrame, airport: str):
        airport_data = self._get_airport_data(airport)
        if airport_data is None:
            warnings.warn("can not push data, no settings are configured for the selected airport!")
            return

        if not set(airport_data['lane_names']).issubset(set(data.columns.values.tolist())):
            warnings.warn("Not all lanes are present in the DF, please check the input!\n Expected lanes: {}"
                          .format(airport_data['lane_names']))
            return

        for index, row in data.iterrows():
            data_point = influxdb_client.Point(measurement_lanes).time(row['status_time'],
                                                                       WritePrecision.S)  # TODO: verify that it doesn't need unix timestamp
            for lane in airport_data['lane_names']:
                data_point.field(lane, row[lane])

            self._influx_writer.write(bucket=airport_data['influx_bucket'], org=self._config['influx']['org'],
                                      record=data_point)
        self._influx_writer.flush()

    def querry_capacity_predictions(self, airport: str, start_time, end_time, prediction_offset):
        airport_data = self._get_airport_data(airport)
        if airport_data is None:
            warnings.warn("can not querry data, no settings are configured for the selected airport!")
            return None

        query = ' from(bucket:"{}")\
        |> range({}: {})\
        |> filter(fn:(r) => r._measurement == "{}")\
        |> filter(fn: (r) => r.prediction_offset == {})\
        |> filter(fn: (r) => r._field == "{}" or r._field == "{}")\
        |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")' \
            .format(airport_data['influx_bucket'],
                    start_time,
                    end_time,
                    measurement_cap_pred,
                    prediction_offset,
                    "predicted_capacity",
                    "prediction_variance"
                    )

        return self._influx_reader.query(org=self._config['influx']['org'], query=query)

    def query_capacity_predictions_for_slotting(self, airport):

        airport_data = self._get_airport_data(airport)
        if airport_data is None:
            warnings.warn("can not querry data, no settings are configured for the selected airport!")
            return None

        query = '''
          import "date"
          import "strings"
          
          from(bucket: "aerology.release")
            |> range(start: -3h, stop: 48h)
            |> filter(fn: (r) => r._measurement == "capacity_prediction")
            |> filter(fn: (r) => r._field == "predicted_capacity")
            
            // drop +00:00 from 10Nov dupes
            |> filter(fn: (r) => strings.strlen(v: r.init_time) <= 19)
            
            // append time to midnight inits
            |> map(fn: (r) => ({r with init_time: 
              if strings.strlen(v: r.init_time) < 19 then
                r.init_time + " 00:00:00"
              else
                r.init_time
              })
            )
            
            // convert init_time to timestamp
            |> map(fn: (r) => ({r with initDtmz: 
              time(v: 
                strings.replace(v: r.init_time, t: " ", u: "T", i: 1) + "Z")
            }))
          
            |> group(columns: ["_time", "model_id"])
            |> sort(columns: ["initDtmz"])
            |> last()
            |> group()
            |> pivot(rowKey: ["_time"], columnKey: ["model_id"], valueColumn: "_value")
          '''

        # query_api.query(org=self._config['influx']['org'], query=query)
        # dataset =[]
        # for table in results:
        #     for record in table.records:
        #         dataset.append([record.get_time(), record.get_value(), record.get_field()])
        #
        # # df = pandas.DataFrame(dataset, columns=['time', 'value', 'model_id'])
        return self._influx_reader.query_data_frame(org=self._config['influx']['org'], query=query)

    def push_flight_calculations(self, data, airport: str):

        airport_data = self._get_airport_data(airport)
        if airport_data is None:
            warnings.warn("can not push data, no settings are configured for the selected airport!")

        for row in data:
            data_point = influxdb_client.Point(delay_calculations) \
                .tag('flight_id', row['flight_id']) \
                .tag('eta_hour', row['eta_hour']) \
                .tag('slt_hour', row['slt_hour']) \
                .field('predicted_delay', row['predicted_delay']) \
                .field('published_delay', row['published_delay']) \
                .time(row['_time'], WritePrecision.S)

            self._influx_writer.write(bucket=airport_data['influx_bucket'], org=self._config['influx']['org'],
                                      record=data_point)

        self._influx_writer.flush()

    def push_demand(self, data, airport: str):

        airport_data = self._get_airport_data(airport)
        if airport_data is None:
            warnings.warn("can not push data, no settings are configured for the selected airport!")
        for index, row in data.iterrows():
            data_point = influxdb_client.Point(measurement_demand) \
                .field('demand', row['demand']) \
                .time(row['valid_time'], WritePrecision.S)

            self._influx_writer.write(bucket=airport_data['influx_bucket'], org=self._config['influx']['org'],
                                      record=data_point)

        self._influx_writer.flush()

    def querry_capacity_measurements(self, airport: str) -> pandas.DataFrame:
        raise ValueError('NOT IMPLEMENTED!')

    def querry_vectoral_capacity_measurements(self, airport: str, start_time, end_time):
        raise ValueError('NOT IMPLEMENTED!')

    def query_lane_status(self, airport: str, start_time, end_time):

        airport_data = self._get_airport_data(airport)
        if airport_data is None:
            warnings.warn("can not querry data, no settings are configured for the selected airport!")
            return None
        end_time = end_time + timedelta(hours=1)  # Plus one because the last hour is excluded.
        query = f' from(bucket:"{airport_data["influx_bucket"]}")\
        |> range(start: {start_time.isoformat() + "Z"}, stop: {end_time.isoformat() + "Z"})\
        |> filter(fn: (r) => r._measurement == "lane_status")'

        results = self._influx_reader.query(org=self._config['influx']['org'], query=query)
        df = self._convert_query_to_dataframe(results)
        return df

    def _get_airport_data(self, airport_name: str):
        for airport in self._config["airports"]:
            if airport["short_name"].casefold() == airport_name.casefold():
                return airport
        return None

    def __load_config(self, path):
        try:
            with open(path, "r") as json_file:
                self._config = json.load(json_file)

        except:
            self._config = None
            return False
        return True

    def __init_db_clients(self):
        self._influx_client = InfluxDBClient(url=self._config['influx']['url'],
                                             token=self._config['influx']['token'],
                                             org=self._config['influx']['org'])
        self._influx_writer = self._influx_client.write_api(write_options=SYNCHRONOUS)
        self._influx_reader = self._influx_client.query_api()

    def __init__(self, path_to_config="res/config_files.json"):
        if not self.__load_config(path_to_config):
            warnings.warn("loading of configuration failed!")
            return

        validate(self._config, configuration_json_schema)
        self.__init_db_clients()

    def _convert_query_to_dataframe(self, results):
        """
        Function that takes a DB Influx query of Runway Status tables and creates pd.DataFrame for inference.

        Parameters
        ----------
        results= list of FluxTable

        Returns
        -------
        df: pd.DataFrame runway_04L', 'runway_04R', 'runway_11'
        The dataframe with the valid_time, runway_04L, runway_04R, runway_11 columns.
        ""

        """
        dataset = []
        for table in results:
            for record in table.records:
                dataset.append([record.get_time(), record.get_value(), record.get_field()])
        dataset = pandas.DataFrame(dataset, columns=['valid_time', 'runway_status', 'runway'])
        dataset = dataset.pivot(index='valid_time', columns='runway', values='runway_status').reset_index(drop=False)
        dataset.valid_time = dataset.valid_time.dt.tz_localize(None)
        return dataset


def predicted_capacity_push_example():
    capacity_df = pandas.DataFrame({
        "predicted_time": [datetime.strptime("09/11/2022 10", "%d/%m/%Y %H"),
                           datetime.strptime("09/11/2022 10", "%d/%m/%Y %H"),
                           datetime.strptime("09/11/2022 11", "%d/%m/%Y %H"), ],
        "predicted_capacity": [100, 200, 300],
        "prediction_variance": [5, 10, 5],
        "prediction_offset": ["+3", "+3", "+3"],
    })

    handler = InfluxDBHandler()
    handler.push_capacity_predictions(capacity_df, "EWR")


def lane_status_push_example():
    lane_df = pandas.DataFrame({
        "status_time": [datetime.strptime("09/11/2022 10", "%d/%m/%Y %H"),
                        datetime.strptime("09/11/2022 10", "%d/%m/%Y %H"),
                        datetime.strptime("09/11/2022 11", "%d/%m/%Y %H"), ],
        "runway_04L": [0, 1, 1],
        "runway_04R": [1, 1, 1],
        "runway_11": [1, 0, 0],
    })

    handler = InfluxDBHandler()
    handler.push_lane_status(lane_df, "EWR")


def measured_capacity_push_example():
    capacity_df = pandas.DataFrame({
        "time": [datetime.strptime("09/11/2022 10", "%d/%m/%Y %H"),
                 datetime.strptime("09/11/2022 10", "%d/%m/%Y %H"),
                 datetime.strptime("09/11/2022 11", "%d/%m/%Y %H"), ],
        "capacity": [100, 200, 300],
    })

    handler = InfluxDBHandler()
    handler.push_capacity_measurements(capacity_df, "EWR")


def querry_predicted_capacity_example():
    handler = InfluxDBHandler()
    begin = datetime.strptime("09/11/2022 09", "%d/%m/%Y %H")
    begin_unix = time.mktime(begin.timetuple())
    end = datetime.strptime("09/11/2022 19", "%d/%m/%Y %H")
    end_unix = time.mktime(end.timetuple())
    data = handler.querry_capacity_predictions("EWR", begin_unix, end_unix, "+1")

    results = []
    for table in data:
        for record in table.records:
            # results.append((record.values.get..)  # see: https://www.influxdata.com/blog/getting-started-with-python-and-influxdb-v2-0/
            pass

    print(results)


def vectoral_capacity_measurements_push_example():
    capacity_df = pandas.DataFrame({
        "time": [datetime.strptime("09/11/2022 10:00", "%d/%m/%Y %H:%M"),
                 datetime.strptime("09/11/2022 10:15", "%d/%m/%Y %H:%M"),
                 datetime.strptime("09/11/2022 10:30", "%d/%m/%Y %H:%M"),
                 datetime.strptime("09/11/2022 10:45", "%d/%m/%Y %H:%M"),
                 datetime.strptime("09/11/2022 11:00", "%d/%m/%Y %H:%M"),
                 datetime.strptime("09/11/2022 11:15", "%d/%m/%Y %H:%M"),
                 datetime.strptime("09/11/2022 11:30", "%d/%m/%Y %H:%M"),
                 datetime.strptime("09/11/2022 11:45", "%d/%m/%Y %H:%M"),
                 datetime.strptime("09/11/2022 12:00", "%d/%m/%Y %H:%M"), ],
        "capacity": [100, 150, 200, 250, 300, 350, 400, 450, 500],
    })

    handler = InfluxDBHandler()
    now = datetime.now()
    handler.push_vectoral_capacity_measurements(capacity_df, now, "EWR", 'APTC')


if __name__ == '__main__':
    # predicted_capacity_push_example()
    # lane_status_push_example()
    # measured_capacity_push_example()
    # querry_predicted_capacity_example()
    vectoral_capacity_measurements_push_example()
