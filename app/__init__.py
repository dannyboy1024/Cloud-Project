from flask import Flask
import base64
import random

global memcache_global, dummyDB

class memcache_structure:
    def __init__(self):
        self.memcache = {}
        self.memcache_size = 10*1024*1024
        self.memcache_mode = "RR"
        self.current_num_items = 0
        self.current_size = 0
        self.num_requests = 0
        self.miss = 0
        self.hit = 0
        self.access_tracker = None
        
    def memcache_invalidate(self, key):
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
        if self.memcache_mode == "RR":
            key_evict = random.choice(list(self.memcache.keys()))
            self.memcache_invalidate(key_evict)
        elif self.memcache_mode == "LRU":
            key_evict = self.access_tracker[0]
            self.memcache_invalidate(key_evict)

    def memcache_reconfigure(self, size, mode):
        self.num_requests = self.num_requests + 1
        while size < self.memcache_size:
            self.memcache_evict()
        self.memcache_size = size
        if self.memcache_mode != mode: 
            self.memcache_mode = mode
            if self.memcache_mode == "RR":
                self.access_tracker = None
            elif self.memcache_mode == "LRU":
                self.access_tracker = []

    def memcache_put(self, key, value): 
        self.num_requests = self.num_requests + 1
        if key in self.memcache:
            self.memcache_invalidate(key)
        while self.current_size + len(value) > self.memcache_size:
            self.memcache_evict()
        self.memcache[key] = value
        self.current_size = self.current_size + len(value)
        self.current_num_items = self.current_num_items + 1
        if self.memcache_mode == "LRU": 
            self.access_tracker.append(key)

    def memcache_clear(self):
        self.num_requests = self.num_requests + 1
        self.memcache.clear()
        self.current_num_items = 0
        self.current_size = 0
        if self.memcache_mode == "LRU": 
            self.access_tracker = []

    def memcache_get(self, key):
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
        self.num_requests = self.num_requests + 1
        return list(self.memcache.keys())
    
    def current_configuration(self):
        full_list = []
        full_list.append(self.memcache_size)
        full_list.append(self.memcache_mode)
        full_list.append(self.current_num_items)
        full_list.append(self.current_size)
        full_list.append(self.num_requests)
        full_list.append(self.miss)
        full_list.append(self.hit)
        return full_list

webapp = Flask(__name__)
memcache_global = memcache_structure();
dummyDB = {}


from app import main




