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
import hashlib

from abc import ABC, abstractmethod

from kafka_pyrallel_consumer.lru_cache import LRUCacheBase, LocalLRUCache


class DedupBase(ABC):
    @abstractmethod
    def is_message_duplicate(self, msg):
        pass


class DedupIgnore(DedupBase):
    def is_message_duplicate(self, msg):
        return False


class DedupLRU(DedupBase):
    def __init__(
        self,
        dedup_by_key: bool = False,
        dedup_by_value: bool = False,
        dedup_max_lru: int = 32768,
        dedup_algorithm: str = "sha256",
        dedupBackendClass: object = None,
    ):
        """
        Default class to deduplicate messages. It uses a local in-memory list.
        That means this method wil not work properly in case of consumer rebalance as
        there will be no cached dedup shared between consumers within the consumer group.
        To allow interoperability across all consumers in a consumer group this class can be
        overriden to have it cache in Redis or any other external storage.
        If this class is overriden make sure to set the right class name on PyrallelConsumer's DedupClass instance variable.

        *dedup_by_key (bool)*
            Deduplicate messages by the Key. The default is `False`.
            To deduplicate messages by Key and Value, set both dedup_by_key and dedup_by_value as True.

        *dedup_by_value (bool)*
            Deduplicate messages by the Value. The default is `False`.
            To deduplicate messages by Key and Value, set both dedup_by_key and dedup_by_value as True.

        *dedup_max_lru (int)*
            Max LRU cache size. The default is 32768.

        *dedup_algorithm (str)*
            Deduplication hashing algorithm to use.
            Options available are: `md5`, `sha1`, `sha224`, `sha256` (default), `sha384`, `sha512`, `sha3_224`, `sha3_256`, `sha3_384`, `sha3_512`.
            To reduce memory footprint, the cached dedup will be a hash of the Key/Value other than the actual data.

        *dedupBackendClass (class instance)*
            Deduplication LRU cache backend class.
            Instance of class where the LRU cache backend is implemented.
            You can set any class instance here, however it must be an abstract class of `LRUCacheBase`.
            By default it will implement an in-memory local LRU cache (`LocalLRUCache` on `kafka_pyrallel_consumer/lru_cache.py`),
            please note that by having it in local memory it will not work properly in case of
            consumer rebalance as there will be no cached dedup shared between consumers within the consumer group.
            To have a more robust and shared LRU caching mechanism you can use `RedisLRUCache` (also on `kafka_pyrallel_consumer/lru_cache.py`),
            or feel free to create your own backend.
        """
        self._dedup_by_key = dedup_by_key == True
        self._dedup_by_value = dedup_by_value == True
        self._check_for_dedup = self._dedup_by_key or self._dedup_by_value
        if self._check_for_dedup:
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

            # Set/validate Dedup class
            self._dedup_backend = (
                LocalLRUCache() if dedupBackendClass is None else dedupBackendClass
            )
            if not isinstance(self._dedup_backend, LRUCacheBase):
                raise ValueError(
                    "dedupBackendClass must be a class instance of the abstract class LRUCacheBase"
                )

            self._dedup_backend._set_dedup_max_lru(dedup_max_lru)
            self._dedup_topics = dict()
            self._dedup_algorithm = DEDUP_ALGORITHMS.get(dedup_algorithm)
            if self._dedup_algorithm is None:
                error_message = f"Invalid dedup_algorithm '{dedup_algorithm}'. Valid options are: {', '.join(DEDUP_ALGORITHMS.keys())}"
                raise ValueError(error_message)

    def is_message_duplicate(
        self,
        msg,
    ) -> bool:
        """
        Main method to be called.
        In general this method doesn't need to be overriden, only the methods called by it.
        """
        if self._check_for_dedup:
            seed = self._generate_message_seed(msg)

            if seed is not None:
                hash = self._hash_seed(msg, seed)
                return self._is_hash_duplicate(hash)
        return False

    def _generate_message_seed(
        self,
        msg,
    ) -> str:
        """
        Generate message seed based on Key, Value or Key/Value.
        """
        if self._dedup_by_key and self._dedup_by_value:
            return (msg.key() or b"") + b"0" + (msg.value() or b"")
        elif self._dedup_by_key:
            return msg.key() or b""
        elif self._dedup_by_value:
            return msg.value() or b""

    def _hash_seed(
        self,
        msg,
        seed,
    ) -> str:
        """
        Hash the seed, however it will prefix the seed with the message topic.
        That is to avoid clashes between similar messages in different topics.
        If that is not a desired design, please override this class.
        """
        if msg.topic() not in self._dedup_topics:
            self._dedup_topics[msg.topic()] = (
                msg.topic().encode(
                    "ascii",
                    "ignore",
                )
                + b"0"
            )
        return self._dedup_algorithm(self._dedup_topics[msg.topic()] + seed).hexdigest()

    def _is_hash_duplicate(
        self,
        hash,
    ) -> bool:
        """
        Check if the hash is already in the LRU cache.
        """
        exists = self._dedup_backend.exists(hash)
        if not exists:
            # Hash not in cache, append to the end
            self._dedup_backend.put(hash)
            return False
        return True
