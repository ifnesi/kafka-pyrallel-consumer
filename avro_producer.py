#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright 2020 Confluent Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
import os
import time
import random
import string
import logging
import argparse

from configparser import ConfigParser

from uuid import uuid4

from confluent_kafka import Producer
from confluent_kafka.serialization import (
    StringSerializer,
    SerializationContext,
    MessageField,
)
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer


class DeliveryReport:
    def __init__(self, iterations: int):
        self._iteration = 0
        self._last_report = None
        self._iterations = iterations

    def report(self, err, msg):
        self._iteration += 1
        if err is not None:
            logging.error(
                "Delivery failed for User record {}: {}".format(msg.key(), err)
            )
        else:
            progress_bar = int(100 * self._iteration / self._iterations)
            if progress_bar % 10 == 0 and progress_bar != self._last_report:
                self._last_report = progress_bar
                logging.info(f"Progress: {progress_bar}%")


def main(args):
    kconfig = ConfigParser()
    kconfig.read(os.path.join(args.config_filename))

    # Configure Kafka consumer
    conf_confluent = {
        "client.id": args.client_id,
    }
    conf_confluent.update(dict(kconfig["kafka"]))
    producer = Producer(conf_confluent)

    # Configure SR client
    sr_conf = dict(kconfig["schema-registry"])
    sr_client = SchemaRegistryClient(sr_conf)
    string_serializer = StringSerializer("utf_8")
    with open(os.path.join("schemas", "payload.avro"), "r") as f:
        avro_serializer = AvroSerializer(
            sr_client,
            f.read(),
        )

    delivery_report = DeliveryReport(args.iterations)
    for _ in range(args.iterations):
        producer.poll(0.0)
        try:

            key = "".join(random.choices(string.digits, k=2))
            message = {"payload": uuid4().hex, "timestamp": int(time.time() * 1000)}

            producer.produce(
                topic=args.topic,
                key=string_serializer(key),
                value=avro_serializer(
                    message,
                    SerializationContext(
                        args.topic,
                        MessageField.VALUE,
                    ),
                ),
                on_delivery=delivery_report.report,
            )
        except KeyboardInterrupt:
            break
        except Exception as err:
            logging.error(err)
        finally:
            time.sleep(args.interval / 1000)

    producer.flush()


if __name__ == "__main__":
    logging.basicConfig(
        format="%(asctime)s.%(msecs)03d [%(levelname)s]: %(message)s",
        level=logging.INFO,
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    DEFAULT_CONFIG = os.path.join("config", "localhost.ini")
    parser = argparse.ArgumentParser(description="Python Avro producer")
    parser.add_argument(
        "--topic",
        help="Topic name",
        dest="topic",
        type=str,
        default="demo_parallel_consumer",
    )
    parser.add_argument(
        "--config-filename",
        dest="config_filename",
        type=str,
        help=f"Select config filename for additional configuration, such as credentials (default: {DEFAULT_CONFIG})",
        default=DEFAULT_CONFIG,
    )
    parser.add_argument(
        "--kafka-section",
        dest="kafka_section",
        type=str,
        help=f"Section in the config file related to the Kafka cluster (e.g. kafka)",
        default="kafka",
    )
    parser.add_argument(
        "--sr-section",
        dest="sr_section",
        type=str,
        help=f"Section in the config file related to the Schema Registry (e.g. schema-registry)",
        default="schema-registry",
    )
    parser.add_argument(
        "--client-id",
        dest="client_id",
        type=str,
        help=f"Consumer's Client ID (default is 'generic-avro-deserialiser-01')",
        default="generic-avro-deserialiser-01",
    )
    parser.add_argument(
        "--iterations",
        help="Number of messages to be sent (default: 50)",
        dest="iterations",
        default=50,
        type=int,
    )
    parser.add_argument(
        "--interval",
        help="Max interval between messages in milliseconds (default: 1))",
        dest="interval",
        default=1,
        type=int,
    )

    main(parser.parse_args())
