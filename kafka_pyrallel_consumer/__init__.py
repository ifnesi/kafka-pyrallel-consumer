import queue
import random
import logging
import binascii
import threading

from confluent_kafka import Consumer


class PyrallelConsumer(Consumer):
    def __init__(
        self,
        *args,
        record_handler,
        max_concurrency: int = 10,
        ordering: bool = True,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self._ordering = ordering
        self._record_handler = record_handler
        self._max_concurrency = max_concurrency
        self._queue_id = random.randrange(0, max_concurrency - 1)
        self._stop = False

        # Create consumer queues and start consumer threads
        self._threads = list()
        self._queues = list()
        for n in range(max_concurrency):
            self._queues.append(queue.Queue())
            self._threads.append(threading.Thread(target=self._processor, args=(n,)))
        for n, thread in enumerate(self._threads):
            logging.info(f"Starting consumer thread: {n}")
            thread.start()

    def _processor(self, n: int):
        while True:
            is_empty = self._queues[n].empty()
            if is_empty and self._stop:
                logging.info(f"Stopped consumer thread: {n}")
                break
            elif not is_empty:
                msg = self._queues[n].get()
                self._record_handler(msg)

    def stop(self):
        logging.info("Stopping threads")
        self._stop = True
        for thread in self._threads:
            thread.join()
        logging.info("All threads stopped")

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
