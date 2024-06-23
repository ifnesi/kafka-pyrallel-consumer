import queue
import random
import logging
import binascii
import threading

from confluent_kafka import Consumer


def default_handler(msg):
    return msg


class PyrallelConsumer(Consumer):
    def __init__(
        self,
        *args,
        ordering: bool = True,
        max_concurrency: int = 3,
        record_handler: object = default_handler,
        **kwargs,
    ):
        """
        This a wrapper around the Python `Consumer` class (`confluent_kafka` Python lib) called `PyrallelConsumer`. It works similarly to the standard `Consumer` class, however it takes three additional (optional) parameters:
        - `max_concurrency` (int): Number of concurrent threads to handle the consumed messages, default is 3
        - `ordering` (bool): If set to True (default) it will partition the message key (CRC32) and send to the corresponding thread, so it can guarantee message order, meaning, same queue will always process the same message key (within the sdame partition). If set to False, it will randomly allocate the first key to one of the threads then the subsequent keys will be allocated in a round-robin fashion
        - `record_handler` (function): Function to process the messages within each thread. It takes only one parameter `msg` (as returned from a `consumer.poll` call)
        """
        super().__init__(*args, **kwargs)
        self._ordering = ordering == True
        self._record_handler = record_handler
        self._max_concurrency = max(1, int(max_concurrency))
        self._queue_id = random.randint(0, 99999)
        self._stop = False

        # Create consumer queues and start consumer threads
        self._threads = list()
        self._queues = list()
        for n in range(max_concurrency):
            self._queues.append(queue.Queue())
            self._threads.append(
                threading.Thread(
                    name=f"Thread_{n}",
                    target=self._processor,
                    args=(n,),
                )
            )
        for n, thread in enumerate(self._threads):
            logging.info(f"Starting parallel consumer thread: {n}")
            thread.start()

    def _processor(self, queue_id: int):
        while True:
            is_empty = self._queues[queue_id].empty()
            if is_empty and self._stop:
                logging.info(f"Stopped consumer thread: {queue_id}")
                break
            elif not is_empty:
                msg = self._queues[queue_id].get()
                self._record_handler(msg)

    def close(self):
        logging.info("Stopping parallel consumer threads")
        self._stop = True
        for thread in self._threads:
            thread.join()
        logging.info("All parallel consumer threads stopped")
        # Stop threads and close consumer group
        super().close()

    def poll(self, *args, **kwargs):
        if not self._stop:
            msg = super().poll(*args, **kwargs)
            if msg is not None:
                if self._ordering and msg.key() is not None:
                    self._queue_id = binascii.crc32(msg.key()) % self._max_concurrency
                else:
                    self._queue_id = (self._queue_id + 1) % self._max_concurrency
                self._queues[self._queue_id].put(msg)
            return msg
