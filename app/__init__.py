from flask import Flask
from flask_cors import CORS
#import mysql.connector
#from mysql.connector import (connection, errorcode)
import base64
import random

global memcache
memcache = {}

class memcache_structure:
    def __init__(self):
        """
        Constructor, default configuration is 10MB and random replacement mode
        """
        self.memcache = {}
        self.key_size = {}
        self.memcache_size = 10 * 1024 * 1024
        self.memcache_mode = "RR"
        self.current_num_items = 0
        self.current_size = 0
        self.num_requests = 0
        self.miss = 0
        self.hit = 0
        self.cache_operation = True
        # Only used in LRU mode, will be a list, the least recently used 
        # item will be stored in 0 index place, with later entries are the
        # ones accessed more recently
        self.access_tracker = None

    def memcache_invalidate(self, key):
        """
        Remove a key and its entry (value) from the memcache
        """
        if key in self.memcache:
            self.current_size = self.current_size - len(self.memcache[key])
            self.current_num_items = self.current_num_items - 1
            del self.memcache[key]
            if self.memcache_mode == "LRU":
                self.access_tracker.remove(key)
            return "OK"
        else:
            return "Unknown key"

    def memcache_evict(self):
        """
        Called either the remaining size of memcache is smaller than the size of new
        entry, or reconfiguration cause new memcache size smaller than the current size 
        in the memcache
        Will remove one file from the memcache according to the memcache mode
        """
        if self.current_num_items <= 1:
            self.memcache_clear()
        elif self.memcache_mode == "RR":
            keys_in_memcache = list(self.memcache.keys())
            key_evict = random.choice(keys_in_memcache)
            self.memcache_invalidate(key_evict)
        elif self.memcache_mode == "LRU":
            key_evict = self.access_tracker[0]
            self.memcache_invalidate(key_evict)

    def memcache_reconfigure(self, size_MB, mode):
        """
        Reconfigure the size and replacement policy of the memcache, and do necessay self-adjustments
        """
        self.num_requests = self.num_requests + 1
        if size_MB != self.memcache_size:
            size = size_MB * 1024 * 1024
            while size < self.current_size:
                self.memcache_evict()
            self.memcache_size = size
        if self.memcache_mode != mode:
            self.memcache_mode = mode
            if self.memcache_mode == "RR":
                self.access_tracker = None
            elif self.memcache_mode == "LRU":
                self.access_tracker = []

    def memcache_put(self, key, value):
        """
        Insert a new entry into the memcache
        """
        self.num_requests = self.num_requests + 1
        if len(value) > self.memcache_size:
            return "Size too big"
        # Remove old entry if key exist
        if key in self.memcache:
            self.memcache_invalidate(key)
        # If remaining size is not enough, need to evict some entries first
        while self.current_size + len(value) > self.memcache_size:
            self.memcache_evict()
        self.memcache[key] = value
        self.current_size = self.current_size + len(value)
        self.current_num_items = self.current_num_items + 1
        if self.memcache_mode == "LRU":
            self.access_tracker.append(key)
        return "OK"

    def memcache_clear(self):
        """
        Remove every key-value pair in the memcache
        """
        self.num_requests = self.num_requests + 1
        self.memcache.clear()
        self.current_num_items = 0
        self.current_size = 0
        if self.memcache_mode == "LRU":
            self.access_tracker = []

    def memcache_operating(self, operation):
        """
        Turn on or turn off cache operation
        """
        self.cache_operation = operation
        if self.cache_operation == False:
            self.memcache_clear()

    def memcache_get(self, key):
        """
        Return the corresponding value of a given key in the memcache
        """
        self.num_requests = self.num_requests + 1
        if key in self.memcache:
            self.hit = self.hit + 1
            if self.memcache_mode == "LRU":
                self.access_tracker.remove(key)
                self.access_tracker.append(key)
            return self.memcache[key]
        else:
            self.miss = self.miss + 1
            return None

    def memcache_allkeys(self):
        """
        Return a list of all keys in the memcache
        """
        self.num_requests = self.num_requests + 1
        return list(self.memcache.keys())

    def current_configuration(self):
        """
        Return a list of memcache configuration
        [memcache_size, memcache_mode, current_num_items, current_size, num_requests, miss, hit]
        """
        full_list = []
        full_list.append(self.memcache_size)
        full_list.append(self.memcache_mode)
        full_list.append(self.current_num_items)
        full_list.append(self.current_size)
        full_list.append(self.num_requests)
        full_list.append(self.miss)
        full_list.append(self.hit)
        return full_list

global memcache_global
application = Flask(__name__)
CORS(application)
memcache_global = memcache_structure()

from app import main
