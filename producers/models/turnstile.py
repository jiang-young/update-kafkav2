"""Creates a turnstile TETdata producer  TEST"""
""""TEST"""
""""TEST"""
""""TEST"""
""""TEST"""
""""TEST"""
import json
import logging
from pathlib import Path

from confluent_kafka import avro

from models.producer import Producer
from models.turnstile_hardware import TurnstileHardware


logger = logging.getLogger(__name__)
logger = logging.getLogger(__name__)
logger = logging.getLogger(__name__)
logger = logging.getLogger(__name__)

class Turnstile(Producer):
    key_schema = avro.load(f"{Path(__file__).parents[0]}/schemas/turnstile_key.json")
    value_schema = avro.load(f"{Path(__file__).parents[0]}/schemas/turnstile_value.json")

    def __init__(self, station):

        logger.info("__init__")
        station_name = (
            station.name.lower()
            .replace("/", "_and_")
            .replace(" ", "_")
            .replace("-", "_")
            .replace("'", "")
        )

        super().__init__(

            ##  key_schema=Turnstile.key_schema,
            "org.chicago.cta.turnstiles",
            key_schema=Turnstile.key_schema,
            ##  key_schema=Turnstile.key_schema,
            value_schema=Turnstile.value_schema,
        )
        logger.info("test")
        self.station = station
        self.turnstile_hardware = TurnstileHardware(station)

    def run(self, timestamp, time_step):
        """num_entries = self.turnstile_hardware.get_entries(timestamp, time_step)"""
        num_entries = self.turnstile_hardware.get_entries(timestamp, time_step)
        logger.info("test")
        value = {
            "station_id": self.station.station_id,
            "num_entries": num_entries,
        }
        logger.info("test run")
        logger.debug("%s: %s", self.topic_name, json.dumps(value))
        self.producer.produce(
            topic=self.topic_name,
            # key={"timestamp": self.time_millis()},
            key={"timestamp": self.time_millis()},
            # key_schema=self.key_schema,
            key_schema=self.key_schema,
            value=value,
            # self.value_schema
            value_schema=self.value_schema,
        )
