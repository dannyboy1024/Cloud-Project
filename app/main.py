import base64

from flask import render_template, url_for, request
from app import application, memcache_structure, memcache_global
from flask import json
import logging
#from pathlib import Path
from apscheduler.schedulers.background import BackgroundScheduler
import os
import time
import atexit
import requests

os_file_path = os.getcwd() + '/fileFolder/'
memcache_host = 'http://127.0.0.1:5000'  # TODO : memcache url


def write_memcache_stats_to_db():
    # We will use this function to implement the timing storage
    print(time.strftime("%A, %d. %B %Y %I:%M:%S %p"))
    
    # get stats
    #numItems = memcache_global.current_num_items
    #totalSize = memcache_global.current_size
    #numReqs = memcache_global.num_requests
    #numMiss = memcache_global.miss
    #numHit = memcache_global.hit
    #missRate = (numMiss / (numMiss + numHit)) if numMiss + numHit > 0 else 0.0
    #hitRate = (numHit / (numMiss + numHit)) if numMiss + numHit > 0 else 0.0
    
    # update db
    #numRows, lastRowID = db.insertCacheStats(CACHESTATS(numItems, totalSize, numReqs, missRate, hitRate))
    #if numRows == db.cacheStatsTableMaxRowNum:
        # delete the first row if there are 120 rows (10 min stats) in the table
        #db.delCacheStats(lastRowID - db.cacheStatsTableMaxRowNum)
    

scheduler = BackgroundScheduler()
scheduler.add_job(func=write_memcache_stats_to_db, trigger="interval", seconds=5)
scheduler.start()

# Shut down the scheduler when exiting the app
atexit.register(lambda: scheduler.shutdown())


@application.route('/')
def main():
    return render_template("main.html")


@application.route('/get', methods=['POST'])
def get():
    """
    Fetch the value from the memcache given a key
    key: string
    """
    key = request.args.get('key')
    value = memcache_global.memcache_get(key)

    if value is None:
        response = application.response_class(
            response=json.dumps("Unknown key"),
            status=400,
            mimetype='application/json'
        )
    else:
        response = application.response_class(
            response=json.dumps(value),
            status=200,
            mimetype='application/json'
        )
    return response


@application.route('/put', methods=['POST'])
def put():
    """
    Upload the key/value pair to the memcache
    key: string
    value: string (For images, base64 encoded string)
    """
    key = request.args.get('key')
    value = request.args.get('value')
    feedback = memcache_global.memcache_put(key, value)

    status_feedback = 200
    if feedback == "Size too big":
        status_feedback = 400

    response = application.response_class(
        response=json.dumps(feedback),
        status=status_feedback,
        mimetype='application/json'
    )
    return response


@application.route('/clear', methods=['POST'])
def clear():
    """
    Remove all contents in the memcache
    No inputs required
    """
    memcache_global.memcache_clear()

    response = application.response_class(
        response=json.dumps("OK"),
        status=200,
        mimetype='application/json'
    )
    return response


@application.route('/invalidateKey', methods=['POST'])
def invalidateKey():
    """
    Remove an entry in the memcache given a key
    key: string
    """
    key = request.form.get('key')
    message = memcache_global.memcache_invalidate(key)
    if message == "OK":
        response = application.response_class(
            response=json.dumps("OK"),
            status=200,
            mimetype='application/json'
        )
    else:
        response = application.response_class(
            response=json.dumps("Unknown key"),
            status=400,
            mimetype='application/json'
        )
    return response

@application.route('/allKeyMemcache', methods=['GET'])
def allKeyMemcache():
    """`    `
    Display all the keys that stored in the memcache
    No inputs required
    """
    allKeys = memcache_global.memcache_allkeys()
    response = application.response_class(
        response=json.dumps(allKeys),
        status=200,
        mimetype='application/json'
    )
    return response

@application.route('/configureMemcache', methods=['POST'])
def configureMemcache():
    """
    Send the new memcache configuration to the database
    size: an integer, number of MB (eg. 5 will be treated as 5 MB)
    mode: a string, can be either "RR" or "LRU"
    """
    size = request.args.get('size')
    mode = request.args.get('mode')
    configs = {
        'size' : size,
        'mode' : mode
    }
    memcache_global.memcache_reconfigure(size, mode)
    response = application.response_class(
        response=json.dumps(configs),
        status=200,
        mimetype='application/json'
    )
    return response

@application.route('/currentConfig', methods=['GET'])
def currentConfig():
    """
    Display memcache related configuration and statistics tracked by memcache locally
    No inputs required
    """
    configurationList = memcache_global.current_configuration()
    response = application.response_class(
        response=json.dumps(configurationList),
        status=200,
        mimetype='application/json'
    )
    return response
