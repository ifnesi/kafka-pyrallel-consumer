import time
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
        This a wrapper around the Python Consumer class (confluent_kafka Python lib) called PyrallelConsumer. It works similarly to the standard Consumer class, however it takes three additional (optional) parameters:
        *max_concurrency (int)*
            Number of concurrent threads to handle the consumed messages, default is 3
        *ordering (bool)*
            If set to True (default) it will partition the message key (CRC32) and
            send to the corresponding thread, so it can guarantee message order
            meaning, same queue will always process the same message key (within the sdame partition)
            If set to False, it will randomly allocate the first key to one of the threads then
            the subsequent keys will be allocated in a round-robin fashion
        *record_handler (function)*
            Function to process the messages within each thread
            It takes only one parameter msg (as returned from a consumer.poll call)
        """
        # Call original Consumer class method
        super().__init__(*args, **kwargs)

        # Set wrapper instance variables
        self._ordering = ordering == True
        self._record_handler = record_handler
        self._max_concurrency = max(1, int(max_concurrency))
        self._queue_id = random.randint(0, 99999)
        self._stop = False
        self._paused = False
        self.last_msg = None
        self.last_msg_timestamp = -1
        self.last_commit_timestamp = -1

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
        """
        Execute the record_handler under each thread!
        """
        while True:
            is_empty = self._queues[queue_id].empty()
            if is_empty and self._stop:
                logging.info(f"Stopped consumer thread: {queue_id}")
                break
            elif not is_empty:
                msg = self._queues[queue_id].get()
                self._record_handler(msg)

    def commit(self, *args, **kwargs):
        """
        Overriding the original consumer poll method
        if asynchronous is set as True it will wait all queue(s) to be empty
        before sending the commit
        """
        self._paused = True
        if kwargs.get("asynchronous") == False:
            # If commit is synchronous (asynchronous = False) it will wait all queues to be empty
            # only then will issue the commit
            for n, queue in enumerate(self._queues):
                logging.info(f"Waiting for queue #{n} to be empty before committing...")
                while not queue.empty():
                    pass

        # Call original Consumer class method
        super().commit(*args, **kwargs)
        self.last_commit_timestamp = time.time()
        self._paused = False

    def poll(self, *args, **kwargs):
        """
        Overriding the original consumer poll method
        It will poll Kafka and send the message to the corresponding queue/thread
        """
        if not (self._stop or self._paused):
            # Call original Consumer class method
            msg = super().poll(*args, **kwargs)

            # Send message to the corresponding queue/thread
            if msg is not None:
                if self._ordering and msg.key() is not None:
                    self._queue_id = binascii.crc32(msg.key()) % self._max_concurrency
                else:
                    self._queue_id = (self._queue_id + 1) % self._max_concurrency
                self._queues[self._queue_id].put(msg)
                self.last_msg = msg
                self.last_msg_timestamp = msg.timestamp()
            return msg

    def close(self, *args, **kwargs):
        """
        Overriding the original consumer close method.
        It will stop all queues/threads and only then call the close original method
        """
        # Send signal to stop threads (it will do so once all queues are empty)
        logging.info("Stopping parallel consumer threads")
        self._stop = True
        for thread in self._threads:
            thread.join()
        logging.info("All parallel consumer threads stopped")

        # Stop threads and close consumer group by calling original Consumer class method
        super().close(*args, **kwargs)
