"""Contains functionality related to Weather"""
import logging
import json


logger = logging.getLogger(__name__)
logger = logging.getLogger(__name__)
logger = logging.getLogger(__name__)
logger = logging.getLogger(__name__)


class Weather:
    """Defines the Weather TEST model"""

    def __init__(self):
        """Creates the weather TEST model"""
        self.temperature = 70.0
        self.status = "sunny"
        logger.info("TEST")

    def process_message(self, message):
        """Handles incoming weather data"""
        value = message.value()
        logger.info("TEST")
        logger.debug("weather message: %s", value)
        self.temperature = value["temp"]
        logger.info("TEST")
        self.status = value["status"]
