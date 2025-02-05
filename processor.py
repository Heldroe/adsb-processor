#!/usr/bin/env python3

import argparse
from dataclasses import dataclass, field
import json
import signal
import sys
from typing import Tuple, Optional

from kafka import KafkaConsumer, KafkaProducer
import pyModeS as pms

from adsb_protobufs.protocols import adsb_pb2


parser = argparse.ArgumentParser(description="ADSB Processor: decode ADSB frames from Kafka and publish decoded messages.")
parser.add_argument(
    '--bootstrap-servers',
    type=str,
    required=True,
    help='Comma-separated list of Kafka bootstrap servers (host:port).',
)
parser.add_argument(
    '--input-topic',
    type=str,
    default='adsb.frames',
    help='Kafka topic to publish Beast frames to.',
)
parser.add_argument(
    '--group-id',
    type=str,
    default='adsb-processor',
    help='Kafka consumer group ID.',
)
args = parser.parse_args()


def is_even(frame):
    return pms.hex2bin(frame)[53] == "0"


def is_odd(frame):
    return not is_even(frame)


@dataclass
class FlightData:
    icao: str
    last_odd: Optional[Tuple[float, str]] = field(default=None, repr=False)
    last_even: Optional[Tuple[float, str]] = field(default=None, repr=False)

    def handle_frame(self, timestamp, frame):
        if is_even(frame):
            self.last_even = (timestamp, frame)
        else:
            self.last_odd = (timestamp, frame)

    def get_position(self):
        if self.last_even != None and self.last_odd != None:
            return pms.adsb.airborne_position(
                self.last_even[1],
                self.last_odd[1],
                self.last_even[0],
                self.last_even[0],
            )


def get_or_create_flight_data(registry, icao):
    if icao not in registry:
        registry[icao] = FlightData(icao)
    return registry[icao]


def graceful_shutdown(signum, frame):
    print(f"Received signal {signum}, shutting down gracefully...")
    print("Closing Kafka consumer...")
    kafka_consumer.close()
    print("Flushing Kafka producer...")
    kafka_producer.flush()
    sys.exit(0)


signal.signal(signal.SIGINT, graceful_shutdown)  # Handle Ctrl+C
signal.signal(signal.SIGTERM, graceful_shutdown) # Handle termination signal

kafka_consumer = KafkaConsumer(
    args.input_topic,
    bootstrap_servers=args.bootstrap_servers,
    group_id=args.group_id,
    auto_offset_reset='latest',
    auto_commit_interval_ms=1000,
)
kafka_producer = KafkaProducer(
    bootstrap_servers=args.bootstrap_servers,
    compression_type='gzip',
    value_serializer=lambda v: v.SerializeToString(),
)

SPEED_TYPES = {
    "GS": adsb_pb2.AirborneVelocity.SpeedType.GROUND_SPEED,
    "IAS": adsb_pb2.AirborneVelocity.SpeedType.INDICATED_AIR_SPEED,
    "TAS": adsb_pb2.AirborneVelocity.SpeedType.TRUE_AIR_SPEED,
}

flight_registry = {}

for msg in kafka_consumer:
    icao = msg.key.hex()
    frame = adsb_pb2.ADSBFrame.FromString(msg.value)
    frame_data = frame.frame_data.hex()

    tc = pms.adsb.typecode(frame_data)

    if (tc >= 9 and tc <= 18) or (tc >= 20 and tc <= 22):
        fd = get_or_create_flight_data(flight_registry, icao)
        fd.handle_frame(msg.timestamp / 1000, frame_data)
        if position := fd.get_position():
            kafka_producer.send('airborne_position.raw',
                key=msg.key,
                value=adsb_pb2.AirbornePosition(
                    source_id=frame.source_id,
                    latitude=position[0],
                    longitude=position[1],
                    altitude=pms.adsb.altitude(frame_data),
                )
            )
    elif tc == 19:
        av = pms.adsb.airborne_velocity(frame_data)
        kafka_producer.send('airborne_velocity.raw',
            key=msg.key,
            value=adsb_pb2.AirborneVelocity(
                source_id=frame.source_id,
                speed=av[0],
                heading=av[1],
                vertical_speed=av[2],
                speed_type=SPEED_TYPES[av[3]],
            )
        )
    elif tc >= 1 and tc <= 4:
        kafka_producer.send('aircraft_identification.raw',
            key=msg.key,
            value=adsb_pb2.AircraftIdentification(
                source_id=frame.source_id,
                call_sign=pms.adsb.callsign(frame_data),
                category=pms.adsb.category(frame_data),
            )
        )
