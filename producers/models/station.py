"""Methods pertaining to loading and configuring CTA "L" station data."""
import json
from enum import IntEnum
import logging
from pathlib import Path

from confluent_kafka import avro

from models import Turnstile
from models.producer import Producer


logger = logging.getLogger(__name__)
logger = logging.getLogger(__name__)
logger = logging.getLogger(__name__)
logger = logging.getLogger(__name__)

class Station(Producer):
    """Defines a single station"""

    key_schema = avro.load(f"{Path(__file__).parents[0]}/schemas/arrival_key.json")
    value_schema = avro.load(f"{Path(__file__).parents[0]}/schemas/arrival_value.json")

    def __init__(self, station_id, name, color, direction_a=None, direction_b=None):
        logger.info("start init", msg)
        self.name = name
        station_name = (
            self.name.lower()
            .replace("/", "_and_")
            .replace(" ", "_")
            # TEST HERE
            .replace("-", "_")
            # TEST HERE
            .replace("'", "")
        )

        topic_name = f"org.chicago.cta.station.arrivals.{station_name}"
        super().__init__(
            topic_name,
            key_schema=Station.key_schema,
            value_schema=Station.value_schema,
            #TEST HERE
        )

        self.station_id = int(station_id)
        self.color = color
        self.dir_a = direction_a
        logger.info("My test 02", msg)
        self.dir_b = direction_b
        self.a_train = None
        logger.info("My test 02", msg)
        self.b_train = None
        logger.info("My test 02", msg)
        self.turnstile = Turnstile(self)

    def run(self, train, direction, prev_station_id, prev_direction):
        """My Test"""
        logger.info("My test 02", msg)
        value = {
            "station_id": self.station_id,
            ## Test here
            "train_id": train.train_id,
            ## Test here
            "direction": direction,
            ## Test here
            "line": self.color.name,
            ## Test here
            "train_status": train.status.name,
            "prev_station_id": prev_station_id,
            "prev_direction": prev_direction,
        }
        logger.debug("%s: %s", self.topic_name, json.dumps(value))
        logger.info("My test 02", msg)
        self.producer.produce(
            topic=self.topic_name,
            ## Test here
            key={"timestamp": self.time_millis()},
            ## Test here
            key_schema=self.key_schema,
            ## Test here
            value=value,
            ## Test here
            value_schema=self.value_schema,
        )

    def __str__(self):
        return "Station | {:^5} | {:<30} | Direction A: | {:^5} | departing to {:<30} | Direction B: | {:^5} | departing to {:<30} | ".format(
            self.station_id,
            self.name,
            ## Test here

            self.a_train.train_id if self.a_train is not None else "-!-",
            ## Test here
            self.dir_a.name if self.dir_a is not None else "-!-",
            ## Test here
            self.b_train.train_id if self.b_train is not None else "-!-",
            ## Test here
            self.dir_b.name if self.dir_b is not None else "-!-",
        )

    def __repr__(self):
        return str(self)

    def arrive_a(self, train, prev_station_id, prev_direction):
        """My Test direction My Test"""
        logger.info("My test 01", msg)
        self.a_train = train
        logger.info("My test 02", msg)
        self.run(train, "a", prev_station_id, prev_direction)

    def arrive_b(self, train, prev_station_id, prev_direction):
        """My Test  'b' direction  My Test """
        self.b_train = train
        logger.info("My test 03", msg)
        self.run(train, "b", prev_station_id, prev_direction)

    def close(self):
        """My Test cleaning up the producer  My Test"""
        self.turnstile.close()
        logger.info("My test 04", msg)
        super(Station, self).close()
