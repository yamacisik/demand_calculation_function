FROM public.ecr.aws/lambda/python:3.9

COPY calculate_demand.py requirements.txt ./

COPY res/config.json ./res/config.json

COPY dependencies/influxdb-api ./dependencies/influxdb-api

COPY dependencies/utils ./dependencies/utils

RUN python3.9 -m pip install --upgrade pip

RUN python3.9 -m pip install -r requirements.txt -t .

CMD ["calculate_demand.lambda_demand_calculator"]
