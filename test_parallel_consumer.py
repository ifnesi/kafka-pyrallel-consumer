# -*- coding: utf-8 -*-
#
# Copyright 2022 Confluent Inc.
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
import logging
import argparse
import requests

from configparser import ConfigParser
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import (
    SerializationContext,
    MessageField,
    StringDeserializer,
)

from kafka_pyrallel_consumer import PyrallelConsumer


class RecordHandler:
    def __init__(self, key_deserialiser, value_deserialiser, url: str):
        self._key_deserialiser = key_deserialiser
        self._value_deserialiser = value_deserialiser
        self.url = url

    def postmanEcho(self, msg) -> dict:
        if msg.error():
            logging.error(msg.error())
        else:
            key = self._key_deserialiser(msg.key())
            message = self._value_deserialiser(
                msg.value(),
                SerializationContext(
                    msg.topic(),
                    MessageField.VALUE,
                ),
            )
            response = requests.post(
                url=f"{self.url}?key={key}",
                json=message,
            )
            logging.info(response.json())


def main(args):
    kconfig = ConfigParser()
    kconfig.read(os.path.join(args.config_filename))

    # Configure SR client
    string_deserializer = StringDeserializer("utf_8")
    sr_conf = dict(kconfig["schema-registry"])
    sr_client = SchemaRegistryClient(sr_conf)
    avro_deserializer = AvroDeserializer(
        sr_client,
    )

    record_handler = RecordHandler(
        string_deserializer,
        avro_deserializer,
        "https://postman-echo.com/post",
    )

    # Configure Kafka paralle consumer
    conf_confluent = {
        "group.id": args.group_id,
        "client.id": args.client_id,
        "auto.offset.reset": args.offset_reset,
        "enable.auto.commit": False,
    }
    conf_confluent.update(dict(kconfig["kafka"]))
    consumer = PyrallelConsumer(
        conf_confluent,
        ordering=True,
        max_concurrency=5,
        record_handler=record_handler.postmanEcho,
    )

    try:
        consumer.subscribe([args.topic])
        logging.info(
            f"Started consumer {conf_confluent['client.id']} ({conf_confluent['group.id']}) on topic '{args.topic}'"
        )

        last_msg_timestamp = consumer.last_msg_timestamp
        while True:
            try:
                # Poll kafka, however it will also have the messages processed as per function set on the `record_handler` argument
                consumer.poll(timeout=0.25)
                if (
                    time.time() - consumer.last_commit_timestamp > 5
                ) and last_msg_timestamp != consumer.last_msg_timestamp:
                    consumer.commit(
                        pause_poll=True,
                        asynchronous=False,
                    )
                    last_msg_timestamp = consumer.last_msg_timestamp

            except Exception as err:
                logging.error(err)

    except KeyboardInterrupt:
        logging.info("CTRL-C pressed by user!")

    finally:
        logging.info(
            f"Closing consumer {conf_confluent['client.id']} ({conf_confluent['group.id']})"
        )
        consumer.close()


if __name__ == "__main__":
    logging.basicConfig(
        format="%(asctime)s.%(msecs)03d [%(levelname)s]: %(message)s",
        level=logging.INFO,
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    parser = argparse.ArgumentParser(description="Test parallel consumer")
    DEFAULT_CONFIG = os.path.join("config", "localhost.ini")
    OFFSET_RESET = [
        "earliest",
        "latest",
    ]
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
        "--offset-reset",
        dest="offset_reset",
        help=f"Set auto.offset.reset (default: {OFFSET_RESET[0]})",
        type=str,
        default=OFFSET_RESET[0],
        choices=OFFSET_RESET,
    )
    parser.add_argument(
        "--group-id",
        dest="group_id",
        type=str,
        help=f"Consumer's Group ID (default is 'avro-deserialiser')",
        default="avro-deserialiser",
    )
    parser.add_argument(
        "--client-id",
        dest="client_id",
        type=str,
        help=f"Consumer's Client ID (default is 'avro-deserialiser-01')",
        default="avro-deserialiser-01",
    )

    main(parser.parse_args())
