#!/usr/bin/env python3
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
import time
import queue
import random
import logging
import binascii
import datetime
import threading

from confluent_kafka import Consumer, KafkaException

from kafka_pyrallel_consumer.dedup import DoNotDedup


def default_handler(msg):
    """
    Do nothing, return message as is
    """
    return msg


class PyrallelConsumer(Consumer):
    def __init__(
        self,
        *args,
        ordering: bool = True,
        max_concurrency: int = 3,
        record_handler: object = default_handler,
        max_queue_backlog: int = 1024,
        dedup: object = None,
        **kwargs,
    ):
        """
        This a wrapper around the Python `Consumer` class (`confluent_kafka` Python lib), called `PyrallelConsumer`.

        This wrapper class provides a way to process Kafka messages in parallel, improving efficiency and speed.
        It enables fine-grained control over parallelism beyond the default partition-level in Kafka,
        allowing key-level and message-level parallelism.
        This helps in handling fixed partition counts, integrating with slow databases or services, and
        managing queue-like message processing more effectively.
        The class is designed to optimise performance without requiring extensive changes to your existing Kafka setup.
        It also has the capability to deduplicate messages within the topic partitions it is currently consuming from.

        It works similarly to the standard `Consumer` class but with additional optional parameters:

        *max_concurrency (int)*
            Specifies the number of concurrent threads for handling consumed messages. The default is 3.

        *ordering (bool)*
            When `True` (default), it hashes the message key, divides it, and assigns it to the
            corresponding queue/thread to maintain message order.
            If `False`, it randomly allocates the first message to a queue/thread and
            uses round-robin allocation for subsequent keys.

        *record_handler (function)*
            A function to process messages within each thread,
            taking a single parameter `msg` as returned from a `consumer.poll` call.

        *max_queue_backlog (int)*
            Max number of unprocessed items in the queue(s),
            if that number is reached the polling will be automatically paused and wait for the queue to be cleared.
            The default is 1024.

        *dedup (class instance)*
            Instance of class to do the message deduplication. You can set any class instance here,
            however it must have at least a method called `is_message_duplicate` where its only argument is the Kafka polled message object.
            See example on the class `DedupDefault` (`kafka_pyrallel_consumer/dedup.py`) where it will use an in-memory LRU cache.
            This parameter is optional and if not set will not dedup any message.
        """
        # Call original Consumer class method
        super().__init__(*args, **kwargs)

        # Set wrapper instance variables
        self._ordering = ordering == True
        self._record_handler = record_handler
        self._max_concurrency = max(1, int(max_concurrency))
        self._max_queue_lag = max(1, int(max_queue_backlog))
        self._queue_id = random.randint(0, 999999999)
        self._stop = False
        self._paused = False
        self.last_msg = None  # record a copy of the last message received
        self.last_msg_timestamp = 0  # record when the last message was received
        self.last_commit_timestamp = 0  # record when the last commit was issued (required to synchronous commits)

        if dedup is None:
            self._dedup = DoNotDedup()
        else:
            self._dedup = dedup

        # Create consumer queues and start consumer threads
        self._queues = list()
        self._threads = list()
        for n in range(max_concurrency):
            self._queues.append(queue.Queue())
            self._threads.append(
                threading.Thread(
                    target=self._processor,
                    args=(n,),
                    daemon=False,
                )
            )
        for n, thread in enumerate(self._threads):
            logging.info(f"Starting parallel consumer thread #{n}...")
            thread.start()

    def _has_reached_max_queue_backlog(self) -> bool:
        """
        Check whether queue(s) has reached the max_queue_backlog
        """
        qsize = 0
        for q in self._queues:
            qsize += q.qsize()
            if qsize >= self._max_queue_lag:
                return True  # no need to continue
        return False

    def _processor(
        self,
        n: int,
    ):
        """
        Run the record_handler under each thread!
        """
        while True:
            if not self._queues[n].empty():
                msg = self._queues[n].get()
                self._record_handler(msg)
            elif self._stop:
                logging.info(f"Stopped consumer thread #{n}")
                break
            else:
                # In case queue is empty wait 5ms before checking it again
                time.sleep(0.005)

    def commit(
        self,
        *args,
        pause_poll: bool = False,
        **kwargs,
    ):
        """
        Overriding the original consumer poll method
        if asynchronous is set as False it will wait all queue(s) to be empty before sending the commit.

        *pause_poll (bool)*
            It will pause the poll and resumed only after the commit is issued,
            Default value is False.

        """
        self._paused = (
            pause_poll == True
        )  # when it is paused the poll will be paused until the commit is completed

        if not kwargs.get("asynchronous", True):
            # If commit is synchronous (asynchronous = False) it will wait all queues to be empty
            # only then will issue the commit
            for n, queue in enumerate(self._queues):
                if not queue.empty:
                    logging.info(
                        f"Waiting for queue on thread #{n} to be empty before committing..."
                    )
                    while not queue.empty():
                        pass

        # Call original Consumer class method
        logging.info("Committing last offset...")
        committed = True
        try:
            super().commit(*args, **kwargs)
        except KafkaException as err1:
            try:
                if err1.args[0].code() == -168:  # _NO_OFFSET
                    committed = False
                else:
                    logging.warn(err1)
            except Exception as err1_1:
                logging.error(err1_1)
        except Exception as err2:
            logging.error(err2)
        finally:
            if committed:
                logging.info("Last offset committed!")
                self.last_commit_timestamp = time.time()
            else:
                logging.info(
                    f"No need to commit: Last offset already committed at {datetime.datetime.fromtimestamp(self.last_commit_timestamp)}"
                )

        self._paused = False

    def poll(
        self,
        *args,
        **kwargs,
    ):
        """
        Overriding the original consumer poll method
        It will poll Kafka and send the message to the corresponding queue/thread
        """
        if not (self._stop or self._paused):
            if self._has_reached_max_queue_backlog():
                logging.debug(
                    f"Polling is paused as it has reached the maximum backlog capacity of {self._max_queue_lag} queued items"
                )

            else:
                # Call original Consumer class method
                msg = super().poll(*args, **kwargs)

                # Send message to the corresponding queue/thread
                if msg is not None:

                    if not msg.error():

                        if self._ordering and msg.key() is not None:
                            # CRC32 hash the key and mod divide by the number of queues/threads
                            self._queue_id = (
                                binascii.crc32(msg.key()) % self._max_concurrency
                            )
                        else:
                            # Round-robin queue/thread allocation (first allocation is random)
                            self._queue_id = (
                                self._queue_id + 1
                            ) % self._max_concurrency

                        # Check for duplicated messages
                        if not self._dedup.is_message_duplicate(msg):
                            # Push message to the queue (if not duplicated or dedup check is not required)
                            self._queues[self._queue_id].put(msg)
                            self.last_msg = msg
                            self.last_msg_timestamp = msg.timestamp()

                return msg

    def close(
        self,
        *args,
        graceful_shutdown: bool = True,
        commit_before_closing: bool = True,
        commit_asynchronous: bool = False,
        **kwargs,
    ):
        """
        Overriding the original consumer close method.

        *graceful_shutdown (bool)*
            When `True` (default) it will stop the poll and wait all queues/threads before the consumer leaves the consumer group.

        *commit_before_closing (bool)*
            When `True` (default) it will issue a final commit before the consumer leaves the consumer group.

        *commit_asynchronous (bool)*
            When `False` (default) it will execute a synchronous commit (only applicable if commit_before_closing is `True`).
        """
        self._stop = True

        if graceful_shutdown:
            # Send signal to stop threads (it will do so once all queues are empty)
            logging.info("Stopping all parallel consumer threads...")
            for thread in self._threads:
                thread.join()
            logging.info("All parallel consumer threads have been stopped!")

        if commit_before_closing:
            self.commit(asynchronous=commit_asynchronous)

        # Stop threads and close consumer group by calling original Consumer class method
        super().close(*args, **kwargs)
