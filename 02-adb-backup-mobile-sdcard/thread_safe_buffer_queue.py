# Version 1.4
# CHANGELOG
# 1.4: Changed default queue size to 0 (treated as infinite queue size)
# 1.3: Fixed some occasions that cause inappropriate QueueClosedException
import threading
from typing import Any
from time import time
from or_event import OrEvent


# this exception will be raised while enqueuing objects after queue closed
class QueueClosedException(Exception):
    pass


class OperationTimedOutException(Exception):
    pass


def _wait(wait_func, timeout):
    t = time()
    if not wait_func(timeout=timeout):
        raise OperationTimedOutException()
    return timeout - (time() - t)


class ThreadSafeBufferQueue:
    """
    A helper class providing multi-threaded FIFO queue with specified size,
    enqueue or dequeue operation will be blocked if buffer is full or empty, respectively
    """
    def __init__(self, queue_size: int = 0):
        self._queue_size = queue_size
        self._mutex = threading.RLock()
        self._queue = []
        self._queue_not_full = threading.Event()
        self._queue_not_empty = threading.Event()
        self._queue_closed = threading.Event()
        self._queue_not_full_or_closed = OrEvent(self._queue_not_full, self._queue_closed)
        self._queue_not_empty_or_closed = OrEvent(self._queue_not_empty, self._queue_closed)

    def enqueue(self, obj: Any, timeout: float = 0):
        """
        Enqueue an item to the buffer, operation will be blocked if queue is full
        :param obj: item to be enqueued
        :param timeout: operation timeout value, in seconds
        :return: none
        :exception QueueClosedException: try to enqueue items after queue closed
        :exception OperationTimedOutException: operation timed out when trying to acquire the mutex or waiting
        items to be consumed
        """
        # acquire the mutex lock, and ensure the queue size satisfy the constraint after enqueue
        if timeout <= 0:
            # infinite blocking code segment
            self._mutex.acquire()
            # after acquired the mutex lock, check the queue size
            while len(self._queue) >= self._queue_size > 0:
                # if the queue is currently full, release the mutex lock, wait consumer,
                # and try to acquire the mutex lock again
                self._mutex.release()
                self._queue_not_full_or_closed.wait()
                # queue closed event caught when waiting queue being consumed,
                # raise an exception so the caller can handle it
                if self._queue_closed.is_set():
                    raise QueueClosedException()
                self._mutex.acquire()
        else:
            # specified waiting time code segment, wrap all wait operations
            timeout = _wait(self._mutex.acquire, timeout)
            while len(self._queue) >= self._queue_size > 0:
                self._mutex.release()
                timeout = _wait(self._queue_not_full_or_closed.wait, timeout)
                if self._queue_closed.is_set():
                    raise QueueClosedException()
                timeout = _wait(self._mutex.acquire, timeout)
        self._queue.append(obj)
        if len(self._queue) >= self._queue_size > 0:
            self._queue_not_full.clear()
        self._queue_not_empty.set()
        self._mutex.release()

    def dequeue(self, timeout: float = 0) -> Any:
        """
        dequeue an item from the buffer, operation will be blocked if queue is empty
        :param timeout: operation timed out value, in seconds
        :return: the first item in the buffer array
        :exception QueueClosedException: try to enqueue items after queue closed
        :exception OperationTimedOutException: operation timed out when trying to acquire the mutex or waiting
        items to be consumed
        """
        # acquire the mutex lock, and ensure at least one element is stored in the queue
        if timeout <= 0:
            # infinite blocking code segment
            self._mutex.acquire()
            while len(self._queue) == 0:
                # if queue is empty, release the mutex lock, wait producer, and re-lock the mutex
                self._mutex.release()
                self._queue_not_empty_or_closed.wait()
                self._mutex.acquire()
                if len(self._queue) == 0 and self._queue_closed.is_set():
                    self._mutex.release()
                    raise QueueClosedException()
        else:
            timeout = _wait(self._mutex.acquire, timeout)
            while len(self._queue) == 0:
                self._mutex.release()
                timeout = _wait(self._queue_not_empty_or_closed.wait, timeout)
                timeout = _wait(self._mutex.acquire, timeout)
                if len(self._queue) == 0 and self._queue_closed.is_set():
                    self ._mutex.release()
                    raise QueueClosedException()
        obj = self._queue.pop(0)
        if len(self._queue) == 0:
            self._queue_not_empty.clear()
        self._queue_not_full.set()
        self._mutex.release()
        return obj

    def __len__(self):
        return len(self._queue)

    @property
    def queue_size(self) -> int:
        return self._queue_size

    def set_queue_size(self, new_size: int):
        with self._mutex:
            if new_size > len(self._queue) or new_size <= 0:
                self._queue_not_full.set()
            self._queue_size = new_size

    @property
    def is_closed(self) -> bool:
        return self._queue_closed.is_set()

    def close(self):
        with self._mutex:
            self._queue_closed.set()
