from flask import Flask
from flask_cors import CORS
import mysql.connector
from mysql.connector import (connection, errorcode)
import base64
import random

global memcache
memcache = {}

class FILEINFO:
    def __init__(self, key, location):
        self.key = key
        self.location = location

class CACHECONFIGS:
    def __init__(self, capacity, replacementPolicy):
        self.capacity = capacity
        self.replacementPolicy = replacementPolicy

class CACHESTATS:
    def __init__(self, numItems, totalSize, numReqs, missRate, hitRate):
        self.numItems = numItems
        self.totalSize = totalSize
        self.numReqs = numReqs
        self.missRate = missRate
        self.hitRate = hitRate

class RDBMS:

    # Create a db and 3 tables
    #    Cache Config - Initialize it with a capacity of ... and a random replacement policy
    #    File Info
    #    Cache History Stats
    def __init__(self):
        
        ######  Set some constants ######################
        self.cacheStatsTableMaxRowNum = 120


        connection, cursor = self.connect()
        
        ####### Create a database #######################
        try:
            cursor.execute("CREATE DATABASE A1_RDBMS")
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_DB_CREATE_EXISTS:
                print("Database A1_RDBMS already exists.")
            else:
                raise err
        cursor.execute("Use A1_RDBMS")

        ####### Create tables #########################

        # File Info
        sql = """
        CREATE TABLE fileInfo (
        fileKey VARCHAR(100) NOT NULL,
        location VARCHAR(100) NOT NULL,
        PRIMARY KEY (fileKey)
        );
        """
        try:
            cursor.execute(sql)
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
                print("Table fileInfo already exists.")
            else:
                print(err.msg)

        # Cache Config
        sql = """
        CREATE TABLE cacheConfigs (
        ID INT NOT NULL,
        capacity INT(11) NOT NULL,
        replacementPolicy VARCHAR(100) NOT NULL,
        PRIMARY KEY (ID)
        );
        """
        try:
            cursor.execute(sql)
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
                print("Table cacheConfigs already exists.")
            else:
                print(err.msg)
        # Write initial configs
        sql = """
        INSERT INTO cacheConfigs (ID, capacity, replacementPolicy)
        VALUES (%s, %s, %s)
        """
        vals = (0, 52428800, "RR")
        try:
            cursor.execute(sql, vals)
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_DUP_ENTRY:
                print("Table cacheConfigs already initialized.")
            else:
                print(err.msg)

        # Cache Stats History
        sql = """
        CREATE TABLE cacheStatsHistory (
        ID INT AUTO_INCREMENT,
        numItems INT(11) NOT NULL,
        totalSize INT(11) NOT NULL,
        numReqs INT(11) NOT NULL,
        missRate FLOAT(11) NOT NULL,
        hitRate FLOAT(11) NOT NULL,
        PRIMARY KEY (ID)
        );
        """
        try:
            cursor.execute(sql)
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
                print("Table cacheStatsHistory already exists.")
            else:
                print(err.msg)

        ###### Commit the changes on the 3 tables and disconnct ###
        connection.commit()
        cursor.close()
        connection.close()

    def connect(self, db=None):
        connection = mysql.connector.connect(user='ECE1779', passwd='ECE1779_DB', database=db)
        cursor = connection.cursor()
        return connection, cursor

    #######################################
    ###########    Create    ##############
    ####################################### 
    def insertFileInfo(self, fileInfo):
        
        connection, cursor = self.connect(db='A1_RDBMS')
        
        # Current table is cacheConfigs
        tableName = "fileInfo"
        sql = """
        INSERT INTO {} (fileKey, location)
        VALUES (%s, %s)
        """.format(tableName)
        vals = (fileInfo.key, fileInfo.location)
        cursor.execute(sql, vals)

        # Commit the changes and disconnect
        connection.commit()
        cursor.close()
        connection.close()

    def insertCacheStats(self, cacheStats):

        connection, cursor = self.connect(db='A1_RDBMS')
        
        # Current table is cacheStatsHistory
        tableName = "cacheStatsHistory"

        # Check the number of rows in table
        sql = """
        SELECT COUNT(*) 
        FROM {}
        """.format(tableName)
        cursor.execute(sql)
        numRows = cursor.fetchone()[0]

        # Insert new cache stats and get the id of the new row
        sql = """
        INSERT INTO cacheStatsHistory (numItems, totalSize, numReqs, missRate, hitRate)
        VALUES (%s, %s, %s, %s, %s)
        """
        vals = (cacheStats.numItems, cacheStats.totalSize, cacheStats.numReqs, cacheStats.missRate, cacheStats.hitRate)
        cursor.execute(sql, vals)
        lastRowID = cursor.lastrowid


        # Commit the changes and disconnect
        connection.commit()
        cursor.close()
        connection.close()

        # Return the number of rows and the last row id after insertion is done
        return numRows, lastRowID

    #######################################
    ###########     Read     ##############
    #######################################
    def readFileInfo(self, fileKey):

        connection, cursor = self.connect(db='A1_RDBMS')

        # query
        tableName = "fileInfo"
        sql = """
        SELECT * 
        FROM {}
        WHERE fileKey = %s
        """.format(tableName)
        val = (fileKey,)
        cursor.execute(sql, val)
        record = cursor.fetchone()

        # disconnect
        cursor.close()
        connection.close()

        # get and return file info
        return None if record==None else FILEINFO(*record)

    def readAllFileKeys(self):

        connection, cursor = self.connect(db='A1_RDBMS')

        # query
        tableName = "fileInfo"
        sql = """
        SELECT filekey
        FROM {}
        """.format(tableName)
        cursor.execute(sql)
        records = cursor.fetchall()

        # disconnect
        cursor.close()
        connection.close()

        # get and return all the keys from db 
        return [record[0] for record in records]
    
    def readAllFilePaths(self):

        connection, cursor = self.connect(db='A1_RDBMS')

        # query
        tableName = "fileInfo"
        sql = """
        SELECT location
        FROM {}
        """.format(tableName)
        cursor.execute(sql)
        records = cursor.fetchall()

        # disconnect
        cursor.close()
        connection.close()

        # get and return all the file paths from db 
        return [record[0] for record in records]

    def readCacheConfigs(self):

        connection, cursor = self.connect(db='A1_RDBMS')

        # query
        tableName = "cacheConfigs"
        sql = """
        SELECT capacity, replacementPolicy
        FROM {}
        WHERE ID = 0;
        """.format(tableName)
        cursor.execute(sql)
        record = cursor.fetchall()[0]

        # disconnect
        cursor.close()
        connection.close()

        # get and return the configs from db
        return CACHECONFIGS(*record)

    def readAllStats(self):

        connection, cursor = self.connect(db='A1_RDBMS')

        # query
        tableName = "cacheStatsHistory"
        sql = """
        SELECT numItems, totalSize, numReqs, missRate, hitRate
        FROM {}
        """.format(tableName)
        cursor.execute(sql)
        records = cursor.fetchall()

        # disconnect
        cursor.close()
        connection.close()

        # get and return the configs from db
        return [CACHESTATS(*record) for record in records]

    #######################################
    ###########     Update    #############
    ####################################### 
    def updFileInfo(self, fileInfo):

        connection, cursor = self.connect(db='A1_RDBMS')
        
        # Current table is cacheConfigs
        tableName = "fileInfo"
        sql = """
        UPDATE {}
        SET location = %s
        WHERE fileKey = %s
        """.format(tableName)
        val = (fileInfo.location, fileInfo.key)
        cursor.execute(sql, val)

        # Commit the changes and disconnect
        connection.commit()
        cursor.close()
        connection.close()
    
    def updCacheConfigs(self, cacheConfigs):

        connection, cursor = self.connect(db='A1_RDBMS')
        
        # Current table is cacheConfigs
        tableName = "cacheConfigs"
        sql = """
        UPDATE {}
        SET capacity = %s, replacementPolicy = %s
        WHERE ID = 0
        """.format(tableName)
        val = (cacheConfigs.capacity, cacheConfigs.replacementPolicy)
        cursor.execute(sql, val)

        # Commit the changes and disconnect
        connection.commit()
        cursor.close()
        connection.close()

    #######################################
    ###########     Delete    #############
    #######################################
    def delFileInfo(self, fileKey):
        
        connection, cursor = self.connect(db='A1_RDBMS')
        
        # table fileInfo
        tableName = "fileInfo"
        sql = """
        DELETE FROM {} 
        WHERE fileKey = %s
        """.format(tableName)
        val = (fileKey,)
        cursor.execute(sql, val)

        # Commit the changes and disconnect
        connection.commit()
        cursor.close()
        connection.close()

    def delAllFileInfo(self):

        connection, cursor = self.connect(db='A1_RDBMS')

        # table fileInfo
        tableName = "fileInfo"
        sql = """
        TRUNCATE TABLE {}
        """.format(tableName)
        cursor.execute(sql)

        # disconnect
        connection.commit()
        cursor.close()
        connection.close()

    def delCacheStats(self, ID):

        connection, cursor = self.connect(db='A1_RDBMS')
        
        # Table cacheStastHistory
        tableName = "cacheStatsHistory"
        sql = """
        DELETE FROM {} 
        WHERE ID = %s
        """.format(tableName)
        val = (ID,)
        cursor.execute(sql, val)

        # Commit the changes and disconnect
        connection.commit()
        cursor.close()
        connection.close()

class memcache_structure:
    def __init__(self):
        """
        Constructor, default configuration is 10MB and random replacement mode
        """
        self.memcache = {}
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
        if self.memcache_mode == "RR":
            key_evict = random.choice(list(self.memcache.keys()))
            self.memcache_invalidate(key_evict)
        elif self.memcache_mode == "LRU":
            key_evict = self.access_tracker[0]
            self.memcache_invalidate(key_evict)

    def memcache_reconfigure(self, size_MB, mode):
        """
        Reconfigure the size and replacement policy of the memcache, and do necessay self-adjustments
        """
        self.num_requests = self.num_requests + 1
        size = size_MB * 1024 * 1024
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

global memcache_global, db
application = Flask(__name__)
webapp = application
CORS(webapp)
memcache_global = memcache_structure()
db = RDBMS()



from app import main
