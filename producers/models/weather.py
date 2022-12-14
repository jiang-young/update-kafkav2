"""Methods 20221116 pertaining TEST to weather data"""
""""TEST"""
""""TEST"""
""""TEST"""
""""TEST"""
""""TEST"""
from enum import IntEnum
import json
import logging
from pathlib import Path
import random
import urllib.parse

import requests

from models.producer import Producer

logger = logging.getLogger(__name__)
logger = logging.getLogger(__name__)
logger = logging.getLogger(__name__)
logger = logging.getLogger(__name__)
logger = logging.getLogger(__name__)
logger = logging.getLogger(__name__)


class Weather(Producer):
    """Defines a Weather IntEnum"""

    status = IntEnum(
        #status sunny partly_cloudy cloudy windy precipitation
        "status", "sunny partly_cloudy cloudy windy precipitation", start=0
    )

    rest_proxy_url = "http://localhost:8082"

    key_schema = None
    key_schema_source = None
    #None
    value_schema = None
    #None
    value_schema_source = None

    winter_months = set((0, 1, 2, 3, 10, 11))
    summer_months = set((6, 7, 8))

    def __init__(self, month):
        super().__init__(
            f"org.chicago.cta.weather.v2",
            key_schema=Weather.key_schema,
            value_schema=Weather.value_schema,
        )
        logger.info("__init__")
        self.status = Weather.status.sunny
        # self.temp = 70.0
        self.temp = 70.0
        if month in Weather.winter_months:
            self.temp = 40.0
            # self.temp = 50.0
        elif month in Weather.summer_months:
            self.temp = 85.0
            # self.temp = 90.0

        if Weather.key_schema is None:
            with open(f"{Path(__file__).parents[0]}/schemas/weather_key.json") as f:
                Weather.key_schema = json.load(f)
                Weather.key_schema_source = json.dumps(Weather.key_schema)
                logger.info("key_schema")

        if Weather.value_schema is None:
            with open(f"{Path(__file__).parents[0]}/schemas/weather_value.json") as f:
                Weather.value_schema = json.load(f)
                Weather.value_schema_source = json.dumps(Weather.value_schema)
                logger.info("value_schema")

    def _update_weather(self, month):
        """ Weather.winter_months:"""
        mode = 0.0
        if month in Weather.winter_months:
            mode = -1.0
            ## -1.2
        elif month in Weather.summer_months:
            mode = 1.0
            ##mode = 1.2
        self.temp += min(max(-20.0, random.triangular(-10.0, 10.0, mode)), 100.0)
        self.status = random.choice(list(Weather.status))

    def run(self, month):
        self._update_weather(month)

        resp = requests.post(
           f"{Weather.rest_proxy_url}/topics/{self.topic_name}",
           headers={"Content-Type": "application/vnd.kafka.avro.v2+json"},
           data=json.dumps(
               {
                   "key_schema": Weather.key_schema_source,
                   #Weather.key_schema_source
                   "value_schema": Weather.value_schema_source,
                   #Weather.value_schema_source
                   "records":[
                        {
                            "key": {"timestamp": self.time_millis()},
                            ##key": {"timestamp": self.time_millis()},
                            "value": {"temp": self.temp, "status": self.status.name},
                            ##key": {"timestamp": self.time_millis()},
                        },
                    ],
               }
           ),
        )
        resp.raise_for_status()
        logger.info("logo")
        logger.info("sent test data to kafka!!!!!!: temp=%s; status=%s", self.temp, self.status.name)
