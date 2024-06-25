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


class DoNotDedup:
    def is_message_duplicate(self, _):
        return False


class DedupDefault:
    def __init__(
        self,
        dedup_by_key: bool = False,
        dedup_by_value: bool = False,
        dedup_max_lru: int = 32768,
        dedup_algorithm: str = "sha256",
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
            Max Least Recently Used (LRU) cache size. The default is 32768.

        *dedup_algorithm (str)*
            Deduplication hashing algorithm to use.
            Options available are: `md5`, `sha1`, `sha224`, `sha256` (default), `sha384`, `sha512`, `sha3_224`, `sha3_256`, `sha3_384`, `sha3_512`.
            To reduce memory footprint, the cached dedup will be a hash of the Key/Value other than the actual data.
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
            self._dedup_lru = list()
            self._dedup_topics = dict()
            self._dedup_algorithm = DEDUP_ALGORITHMS.get(dedup_algorithm)
            if self._dedup_algorithm is None:
                error_message = f"Invalid dedup_algorithm '{dedup_algorithm}'. Valid options are: {', '.join(DEDUP_ALGORITHMS.keys())}"
                raise ValueError(error_message)

    def is_message_duplicate(
        self,
        msg,
    ):
        """
        Main method to be called.
        In general this method doesn't need to be overriden, only the methods called by it.
        """
        if self._check_for_dedup:
            seed = self._get_message_seed(msg)

            if seed is not None:
                hash = self._hash_seed(msg, seed)
                return self._is_hash_duplicate(hash)
        return False

    def _get_message_seed(
        self,
        msg,
    ):
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
    ):
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
    ):
        """
        Check if the hash is already in the LRU cache.
        In general this method doesn't need to be overriden, only the methods called by it.
        """
        if self._is_hash_in_cache(hash):
            return True
        else:
            # Not a duplicated message, append to the LRU
            self._is_cache_over_max()
            self._append_to_cache(hash)
            return False

    def _is_hash_in_cache(
        self,
        hash,
    ):
        """
        Check if hash is already present in the LRU cache (in-memory list)
        """
        return hash in self._dedup_lru

    def _is_cache_over_max(self):
        """
        Check if LRU cache (in-memory list) is greater or equal to dedup_max_lru.
        """
        if len(self._dedup_lru) >= self._dedup_max_lru:
            self._pop_first_item()

    def _pop_first_item(self):
        """
        Remove the first (older) element from the LRU cache (in-memory list).
        """
        self._dedup_lru.pop(0)

    def _append_to_cache(self, hash):
        """
        Append a new element to the LRU cache (in-memory list).
        """
        self._dedup_lru.append(hash)
