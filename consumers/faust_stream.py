"""Defines trends calculations for stations"""
import logging
from dataclasses import asdict

import faust


logger = logging.getLogger(__name__)


# Faust will ingest records from Kafka in this format
class Station(faust.Record):
    stop_id: int
    direction_id: str
    stop_name: str
    station_name: str
    station_descriptive_name: str
    station_id: int
    ## tester
    order: int
    red: bool
    blue: bool
    green: bool


# Faust will produce records to Kafka in this format
class TransformedStation(faust.Record):
    station_id: int
    station_name: str
    ## tester
    order: int
    line: str


myapp = faust.App("stations-stream-4", broker="kafka://localhost:9092", store="memory://", topic_partitions=1)
# app进行初始化

topic = myapp.topic("org.chicago.cta.connect.v5.stations", value_type=Station, key_type=None)
out_topic = myapp.topic("org.chicago.cta.stations.all.v5", value_type=TransformedStation, internal=True)
# topic进行初始化




@myapp.agent(topic)
async def station_cleaner(stations):
    async for station in stations:
# 进入逻辑选择
        logger.info("My test", msg)
        if station.red:
            line = 'red'
        elif station.blue:
            line = 'blue'
        elif station.green:
            line = 'green'
            ## tester
        else:
            line = None
# 结束逻辑选择
        logger.info("My test", msg)
        t_station = TransformedStation(
            station_id=station.station_id,
            station_name=station.station_name,
            order=station.order,
            line=line,
        )
# 记录问题
        logger.info("My test", msg)
        logger.debug("t_station info: %s", t_station)
        # table[station.station_id] = t_station
        await out_topic.send(value=t_station)


if __name__ == "__main__":
    myapp.main()
