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
    def __init__(
        self,
        key_deserialiser,
        value_deserialiser,
        url: str,
    ):
        self.url = url
        self._key_deserialiser = key_deserialiser
        self._value_deserialiser = value_deserialiser

    def postmanEcho(self, msg):
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
    kconfig.read(os.path.join(args.config))

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
    consumer_config = {
        "group.id": args.group_id,
        "client.id": args.client_id,
        "auto.offset.reset": args.offset_reset,
        "enable.auto.commit": False,
    }
    consumer_config.update(dict(kconfig["kafka"]))
    consumer = PyrallelConsumer(
        consumer_config,
        ordering=True,
        max_concurrency=5,
        record_handler=record_handler.postmanEcho,
        max_queue_backlog=16,
    )

    try:
        consumer.subscribe([args.topic])
        logging.info(
            f"Started consumer {consumer_config['client.id']} ({consumer_config['group.id']}) on topic '{args.topic}'"
        )

        last_msg_timestamp = consumer.last_msg_timestamp
        while True:
            try:
                # Perform synchronous commit (to be done before the poll)
                # In a regular consumer group the decision of when to commit should be made before polling new messages,
                # as they increase the stored offset that will be committed and we only want to commit once per batch.
                if (
                    time.time() - consumer.last_commit_timestamp > 5
                ) and last_msg_timestamp != consumer.last_msg_timestamp:
                    consumer.commit(
                        pause_poll=True,
                        asynchronous=False,
                    )
                    last_msg_timestamp = consumer.last_msg_timestamp

                # Poll kafka, however it will also have the messages processed as per function set on the `record_handler` argument
                consumer.poll(timeout=0.25)

            except Exception as err:
                logging.error(err)

            except KeyboardInterrupt:
                logging.info("CTRL-C pressed by user!")
                break

    except Exception as err:
        logging.error(err)

    finally:
        logging.info(
            f"Closing consumer {consumer_config['client.id']} ({consumer_config['group.id']})"
        )
        consumer.close(
            graceful_shutdown=True,
            commit_before_closing=True,
            commit_asynchronous=False,
        )


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
        "--config",
        dest="config",
        type=str,
        help=f"Select config filename for additional configuration, such as credentials (default: {DEFAULT_CONFIG})",
        default=DEFAULT_CONFIG,
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
