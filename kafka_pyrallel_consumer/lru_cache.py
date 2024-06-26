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
import redis
import datetime

from abc import ABC, abstractmethod
from collections import OrderedDict


def get_epoch_utc() -> float:
    dt = datetime.datetime.now(datetime.timezone.utc)
    utc_time = dt.replace(tzinfo=datetime.timezone.utc)
    return utc_time.timestamp()


class LRUCacheBase(ABC):
    def __init__(self):
        self.dedup_max_lru = 32768

    def _set_dedup_max_lru(self, dedup_max_lru: int,):
        self.dedup_max_lru = dedup_max_lru

    @abstractmethod
    def exists(
        self,
        key: str,
    ):
        pass

    @abstractmethod
    def put(
        self,
        key: str,
    ):
        pass


class LocalLRUCache(LRUCacheBase):
    """
    Backend class to implement LRU in local memory cache
    """

    def __init__(self):
        super().__init__()
        self.cache = OrderedDict()

    def exists(
        self,
        key: str,
    ):
        if key not in self.cache:
            return False
        else:
            # Recycle Hash, moving it to the end
            self.cache.move_to_end(key)
            return True

    def put(
        self,
        key: str,
    ):
        self.cache[key] = None
        self.cache.move_to_end(key)
        if len(self.cache) > self.dedup_max_lru:
            # Remove LRU item
            self.cache.popitem(last=False)


class RedisLRUCache(LRUCacheBase):
    """
    Backend class to implement LRU in Redis.
    It uses Sorted Sets where the score is the UTC EPOCH timestamp
    """

    def __init__(
        self,
        *args,
        host: str = "localhost",
        port: int = 6379,
        redis_key_name: str = "pyrallel_consumer_lru_cache",
        **kwargs,
    ):
        super().__init__()
        self.redis_client = redis.Redis(
            *args,
            host=host,
            port=port,
            decode_responses=True,
            **kwargs,
        )
        self._redis_key_name = redis_key_name

    def exists(
        self,
        key: str,
    ):
        if (
            self.redis_client.zscore(
                self._redis_key_name,
                key,
            )
            is None
        ):
            return False
        else:
            # Recycle Hash, moving it to the end
            self.redis_client.zrem(
                self._redis_key_name,
                key,
            )
            self.redis_client.zadd(
                self._redis_key_name,
                {key: get_epoch_utc()},
            )
            return True

    def put(
        self,
        key: str,
    ):
        self.redis_client.zadd(
            self._redis_key_name,
            {key: get_epoch_utc()},
        )
        if self.redis_client.zcard(self._redis_key_name) > self.dedup_max_lru:
            # Remove LRU item (lowest score)
            self.redis_client.zpopmin(
                self._redis_key_name,
                1,
            )
