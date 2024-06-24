import time
import queue
import random
import hashlib
import logging
import binascii
import threading

from confluent_kafka import Consumer


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
        dedup_by_key: bool = False,
        dedup_by_value: bool = False,
        dedup_max_lru: int = 32768,
        dedup_algorithm: str = "sha256",
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

        *dedup_by_key (bool)*
            Deduplicate messages by the Key. The default is False.
            To deduplicate messages by Key and Value, set both dedup_by_key and dedup_by_value as True.

        *dedup_by_value (bool)*
            Deduplicate messages by the Value. The default is False.
            To deduplicate messages by Key and Value, set both dedup_by_key and dedup_by_value as True.
        
        *dedup_max_lru (int)*
            Max Least Recently Used (LRU) cache size. The default is 32768.
        
        *dedup_algorithm (str)*
            Deduplication algorithm to use.
            Options available are: md5, sha1, sha224, sha256, sha384, sha3_224, sha3_256, sha3_384, sha3_512, sha512.
            The default is sha256.
        """
        # Call original Consumer class method
        super().__init__(*args, **kwargs)

        # Set wrapper instance variables
        self._ordering = (ordering == True)
        self._record_handler = record_handler
        self._max_concurrency = max(1, int(max_concurrency))
        self._queue_id = random.randint(0, 999999999)
        self._stop = False
        self._paused = False
        self.last_msg = None  # record a copy of the last message received
        self.last_msg_timestamp = -1  # record when the last message was received
        self.last_commit_timestamp = (
            -1
        )  # record when the last commit was issued (required ro synchronous commits)

        # Dedup check
        self._dedup_by_key = (dedup_by_key == True)
        self._dedup_by_value = (dedup_by_value == True)
        self._check_for_dedup = self._dedup_by_key or self._dedup_by_value
        if self._check_for_dedup:
            self._dedup_topics = dict()
            DEDUP_ALGORITHMS = {
                "md5": hashlib.md5,
                "sha1": hashlib.sha1,
                "sha224": hashlib.sha224,
                "sha256": hashlib.sha256,
                "sha384": hashlib.sha384,
                "sha512": hashlib.sha512,
                "sha3_224": hashlib.sha3_224,
                "sha3_256": hashlib.sha3_256,
                "sha3_384": hashlib.sha3_384,
                "sha3_512": hashlib.sha3_512,
            }
            self._dedup_max_lru = max(1, dedup_max_lru)
            self._dedup_lru = list()
            self._dedup_algorithm = DEDUP_ALGORITHMS.get(dedup_algorithm)
            if self._dedup_algorithm is None:
                error_message = f"Invalid dedup_algorithm '{dedup_algorithm}'. Valid options are: {', '.join(DEDUP_ALGORITHMS.keys())}"
                raise ValueError(error_message)

        # Create consumer queues and start consumer threads
        self._queues = list()
        self._threads = list()
        for n in range(max_concurrency):
            self._queues.append(queue.Queue())
            self._threads.append(
                threading.Thread(
                    target=self._processor,
                    args=(n,),
                )
            )
        for n, thread in enumerate(self._threads):
            logging.info(f"Starting parallel consumer thread #{n}...")
            thread.start()

    def _processor(
        self,
        n: int,
    ):
        """
        Execute the record_handler under each thread!
        """
        while True:
            is_empty = self._queues[n].empty()
            if is_empty and self._stop:
                logging.info(f"Stopped consumer thread #{n}")
                break
            elif not is_empty:
                msg = self._queues[n].get()
                self._record_handler(msg)

    def commit(
        self,
        *args,
        pause_poll: bool = False,
        **kwargs,
    ):
        """
        Overriding the original consumer poll method
        if asynchronous is set as False it will wait all queue(s) to be empty before sending the commit

        *pause_poll (bool)*
            It will pause the poll and resumed only after the commit is issued,
            however that is only applicable if asynchronous is set as False
            Default value is False

        """
        if not kwargs.get("asynchronous", True):
            if pause_poll:
                self._paused = True  # when it is paused the poll will be paused until the commit is completed
            # If commit is synchronous (asynchronous = False) it will wait all queues to be empty
            # only then will issue the commit
            for n, queue in enumerate(self._queues):
                logging.info(
                    f"Waiting for queue on thread #{n} to be empty before committing..."
                )
                while not queue.empty():
                    pass

        # Call original Consumer class method
        super().commit(*args, **kwargs)
        self.last_commit_timestamp = time.time()
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
                        self._queue_id = (self._queue_id + 1) % self._max_concurrency

                    # Check for duplicated messages
                    if self._check_for_dedup:
                        if self._dedup_by_key and self._dedup_by_value:
                            seed = (msg.key() or b"") + b"0" + (msg.value() or b"")
                        elif self._dedup_by_key:
                            seed = msg.key() or b""
                        elif self._dedup_by_value:
                            seed = msg.value() or b""
                        else:
                            seed = None

                        if seed is not None:
                            if msg.topic() not in self._dedup_topics:
                                self._dedup_topics[msg.topic()] = msg.topic().encode("ascii", "ignore") + b"0"
                            hash = self._dedup_algorithm(self._dedup_topics[msg.topic()] + seed).hexdigest()
                            if hash in self._dedup_lru:
                                # Do not push the message to the queue as it is duplicated
                                return msg
                            else:
                                # Not a duplicated message, append to the LRU
                                if len(self._dedup_lru) >= self._dedup_max_lru:
                                    self._dedup_lru.pop(0)
                                self._dedup_lru.append(hash)

                    # Push message to the queue (if not duplicated or dedup check is not required)
                    self._queues[self._queue_id].put(msg)
                    self.last_msg = msg
                    self.last_msg_timestamp = msg.timestamp()

            return msg

    def close(
        self,
        *args,
        **kwargs,
    ):
        """
        Overriding the original consumer close method.
        It will stop all queues/threads and only then call the close original method
        """
        # Send signal to stop threads (it will do so once all queues are empty)
        logging.info("Stopping all parallel consumer threads...")
        self._stop = True
        for thread in self._threads:
            thread.join()
        logging.info("All parallel consumer threads have been stopped!")

        # Stop threads and close consumer group by calling original Consumer class method
        super().close(*args, **kwargs)
