import base64

from flask import render_template, url_for, request
from app import webapp, memcache_structure, memcache_global, RDBMS, db, CACHECONFIGS, FILEINFO, CACHESTATS
from flask import json
import logging
from pathlib import Path
from apscheduler.schedulers.background import BackgroundScheduler
import os
import time
import atexit
import requests

os_file_path = os.getcwd() + '/fileFolder/'
# os_file_path = "C:/Users/yuanlidi/ECE1779/A1/localFiles/"
memcache_host = 'http://127.0.0.1:1081'  # TODO : memcache url


def write_memcache_stats_to_db():
    # We will use this function to implement the timing storage
    print(time.strftime("%A, %d. %B %Y %I:%M:%S %p"))
    
    # get stats
    numItems = memcache_global.current_num_items
    totalSize = memcache_global.current_size
    numReqs = memcache_global.num_requests
    numMiss = memcache_global.miss
    numHit = memcache_global.hit
    missRate = (numMiss / (numMiss + numHit)) if numMiss + numHit > 0 else 0.0
    hitRate = (numHit / (numMiss + numHit)) if numMiss + numHit > 0 else 0.0
    
    # update db
    numRows, lastRowID = db.insertCacheStats(CACHESTATS(numItems, totalSize, numReqs, missRate, hitRate))
    if numRows == db.cacheStatsTableMaxRowNum:
        # delete the first row if there are 120 rows (10 min stats) in the table
        db.delCacheStats(lastRowID - db.cacheStatsTableMaxRowNum)
    

scheduler = BackgroundScheduler()
scheduler.add_job(func=write_memcache_stats_to_db, trigger="interval", seconds=5)
scheduler.start()

# Shut down the scheduler when exiting the app
atexit.register(lambda: scheduler.shutdown())


@webapp.route('/')
def main():
    return render_template("main.html")


@webapp.route('/get', methods=['POST'])
def get():
    """
    Fetch the value from the memcache given a key
    key: string
    """
    key = request.args.get('key')
    value = memcache_global.memcache_get(key)

    if value is None:
        response = webapp.response_class(
            response=json.dumps("Unknown key"),
            status=400,
            mimetype='application/json'
        )
    else:
        response = webapp.response_class(
            response=json.dumps(value),
            status=200,
            mimetype='application/json'
        )
    return response


@webapp.route('/put', methods=['POST'])
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

    response = webapp.response_class(
        response=json.dumps(feedback),
        status=status_feedback,
        mimetype='application/json'
    )
    return response


@webapp.route('/clear', methods=['POST'])
def clear():
    """
    Remove all contents in the memcache
    No inputs required
    """
    memcache_global.memcache_clear()

    response = webapp.response_class(
        response=json.dumps("OK"),
        status=200,
        mimetype='application/json'
    )
    return response


@webapp.route('/invalidateKey', methods=['POST'])
def invalidateKey():
    """
    Remove an entry in the memcache given a key
    key: string
    """
    key = request.form.get('key')
    message = memcache_global.memcache_invalidate(key)
    if message == "OK":
        response = webapp.response_class(
            response=json.dumps("OK"),
            status=200,
            mimetype='application/json'
        )
    else:
        response = webapp.response_class(
            response=json.dumps("Unknown key"),
            status=400,
            mimetype='application/json'
        )
    return response


@webapp.route('/refreshConfiguration', methods=['POST'])
def refreshConfiguration():
    """
    Read from the database and configure memcache setting according to 
    memcache configuration data
    No inputs required
    """
    # # Pending DB code
    # size = 50 * 1024 * 1024
    # mode = "RR"
    # # End Pending DB code
    cacheConfigs = db.readCacheConfigs()
    size = cacheConfigs.capacity
    mode = cacheConfigs.replacementPolicy
    configs = {
        "size" : size,
        "mode" : mode
    }

    memcache_global.memcache_reconfigure(size, mode)
    response = webapp.response_class(
        response=json.dumps(configs),
        status=200,
        mimetype='application/json'
    )
    return response

@webapp.route('/cacheOperation', methods=['POST'])
def cacheOperation():
    """
    Turn on or off the operation of memcache
    """
    operation = request.args.get('operation')
    memcache_global.memcache_operating(operation)
    response = webapp.response_class(
        response=json.dumps("OK"),
        status=200,
        mimetype='application/json'
    )

    return response

@webapp.route('/uploadToDB', methods=['POST'])
def uploadToDB():
    """
    Upload the key to the database
    Store the value as a file in the local file system, key as filename
    key: string
    value: string (For images, base64 encoded string)
    """
    key = request.args.get('key')
    value = request.args.get('value')
    full_file_path = os.path.join(os_file_path, key)
    print("upadte DB", full_file_path)
    if os.path.isfile(full_file_path):
        os.remove(full_file_path)
    with open(full_file_path, 'w') as fp:
        fp.write(value)

    # # Omit DB code
    # dummyDB[key] = "yes"
    # # End of using dummyDB
    if db.readFileInfo(key) == None:
        db.insertFileInfo(FILEINFO(key, full_file_path))
    else:
        db.updFileInfo(FILEINFO(key, full_file_path))

    response = webapp.response_class(
        response=json.dumps(full_file_path),
        status=200,
        mimetype='application/json'
    )

    return response


@webapp.route('/getFromDB', methods=['POST'])
def getFromDB():
    """
    Fetch the value (file, or image) from the file system given a key
    key: string
    """

    key = request.args.get('key')
    full_file_path = os.path.join(os_file_path, key)
    if not os.path.isfile(full_file_path):
        response = webapp.response_class(
            response=json.dumps("File Not Found"),
            status=400,
            mimetype='application/json'
        )
    else:
        value = Path(full_file_path).read_text()
        response = webapp.response_class(
            response=json.dumps(value),
            status=200,
            mimetype='application/json'
        )

    return response


@webapp.route('/allKeyDB', methods=['POST'])
def allKeyDB():
    """
    Display all the keys that stored in the database
    No inputs required
    """
    # # Omit DB code
    # allKeys = list(dummyDB.keys())
    # # End of using dummyDB
    allKeys = db.readAllFileKeys()

    response = webapp.response_class(
        response=json.dumps(allKeys),
        status=200,
        mimetype='application/json'
    )

    return response


@webapp.route('/deleteAllFromDB', methods=['POST'])
def deleteAllFromDB():
    """
    Remove all the key and values (files, images) from the database and filesystem
    No inputs required
    """
    allKeys = db.readAllFileKeys()
    db.delAllFileInfo()
    for key in allKeys:
        full_file_path = os.path.join(os_file_path, key)
        if os.path.isfile(full_file_path):
            os.remove(full_file_path)
    response = webapp.response_class(
        response=json.dumps("OK"),
        status=200,
        mimetype='application/json'
    )

    return response


@webapp.route('/list_keys', methods=['POST'])
# return keys list to the web front
def getKeys():
    keys = db.readAllFileKeys()
    print("retrieve keys info: ", keys)

    response = webapp.response_class(
        response=json.dumps({
            "success": "true",
            "keys": keys
        }),
        status=200,
        mimetype='application/json'
    )
    return response


@webapp.route('/allKeyMemcache', methods=['GET'])
def allKeyMemcache():
    """`    `
    Display all the keys that stored in the memcache
    No inputs required
    """
    allKeys = memcache_global.memcache_allkeys()
    response = webapp.response_class(
        response=json.dumps(allKeys),
        status=200,
        mimetype='application/json'
    )
    return response


@webapp.route('/delete_all', methods=['POST'])
# delete keys from database, return the key list after auditing
def deleteKeys():
    res = requests.post(memcache_host + '/deleteAllFromDB')
    if res.status_code == 200:
        jsonString = {
            "success": "true"
        }
    else:
        jsonString = 'Fail'
    response = webapp.response_class(
        response=json.dumps(jsonString),
        status=200,
        mimetype='application/json'
    )
    return response

@webapp.route('/configureMemcache', methods=['POST'])
def configureMemcache():
    """
    Send the new memcache configuration to the database
    size: an integer, number of MB (eg. 5 will be treated as 5 MB)
    mode: a string, can be either "RR" or "LRU"
    """
    size = request.args.get('size')
    mode = request.args.get('mode')
    # omit DB code
    
    db.updCacheConfigs(CACHECONFIGS(size, mode))
    configs = {
        'size' : size,
        'mode' : mode
    }

    response = webapp.response_class(
        response=json.dumps(configs),
        status=200,
        mimetype='application/json'
    )
    return response


@webapp.route('/params', methods=['GET', 'PUT'])
# return mem-cache configuration params
def getParams():
    cacheConfigs = None
    if request.method == 'GET':
        cacheConfigs = db.readCacheConfigs()
    elif request.method == 'PUT':
        data = eval(bytes.decode(request.data))
        params = data.get('params')
        cacheConfigs = CACHECONFIGS(params['size'], params['policy'])
        db.updCacheConfigs(cacheConfigs)
        memcache_global.memcache_operating(params['operation'])
        requests.post(memcache_host + '/refreshConfiguration')
    params = {
        'size': cacheConfigs.capacity,
        'policy': cacheConfigs.replacementPolicy,
        'operation':  memcache_global.cache_operation
    }
    response = webapp.response_class(
        response=json.dumps(params),
        status=200,
        mimetype='application/json'
    )
    return response


@webapp.route('/requestCurrentStat', methods=['GET'])
def requestCurrentStat():
    """
    Display memcache related statistics read from database
    No inputs required
    """
    # # Omit DB code
    # allKeys = list(dummyDB.keys())
    # # End of using dummyDB
    cacheStats = db.readAllStats()
    stats = {
        'numItems' : [record.numItems for record in cacheStats],
        'totalSize' : [record.totalSize for record in cacheStats],
        'numReqs' : [record.numReqs for record in cacheStats],
        'missRate' : [record.missRate for record in cacheStats],
        'hitRate' : [record.hitRate for record in cacheStats]
    }
    response = webapp.response_class(
        response=json.dumps(stats),
        status=200,
        mimetype='application/json'
    )
    return response

# For debugging purpose
@webapp.route('/currentConfig', methods=['GET'])
def currentConfig():
    """
    Display memcache related configuration and statistics tracked by memcache locally
    No inputs required
    """
    configurationList = memcache_global.current_configuration()
    response = webapp.response_class(
        response=json.dumps(configurationList),
        status=200,
        mimetype='application/json'
    )
    return response

@webapp.route('/upload', methods=['POST'])
def uploadImage():
    # upload image with key
    # transfer the bytes into dict

    data = request.form
    key = data.get('key')
    imageContent = data.get('file')
    print(eval(imageContent).get('name'))
    requestJson = {
        'key': key,
        'value': base64.b64encode(str(imageContent).encode())
    }
    if memcache_global.cache_operation:
        requests.post(memcache_host + '/put', params=requestJson)
    requests.post(memcache_host + '/uploadToDB', params=requestJson)
    response = webapp.response_class(
        response=json.dumps({
            "success": "true",
            "key": key
        }),
        status=200,
        mimetype='application/json'
    )
    return response

@webapp.route('/key/<key_value>', methods=['POST'])
def getImage(key_value):
    # get, upload image with key
    # retrieve image
    requestJson = {
        'key': key_value
    }
    res = None
    if memcache_global.cache_operation:
        res = requests.post(memcache_host + '/get', params=requestJson)
    if res or res.status_code == 400:
        # cache misses or do not use cache, query db
        print('cache misses or cache not used, query db')
        res = requests.post(memcache_host + '/getFromDB', params=requestJson)
        if res.status_code == 400:
            content = res.json()
            response = webapp.response_class(
                response=json.dumps({
                    "success": "false",
                    "error": {
                        "code": 400,
                        "message": content
                         }
                }),
                status=200,
                mimetype='application/json'
            )
        else:
            content = base64.b64decode(res.content)
            response = webapp.response_class(
                response=json.dumps({
                    "success": "true",
                    "key": key_value,
                    "content": bytes.decode(content)
                }),
                status=200,
                mimetype='application/json'
            )
            requests.post(memcache_host + '/put', params={
                "key": key_value,
                "value": res.content
            })

        return response
    else:
        print('cache success')
        content = base64.b64decode(res.content)
        response = webapp.response_class(
            response=json.dumps(json.dumps({
                    "success": "true",
                    "key": key_value,
                    "content": bytes.decode(content)
                }),),
            status=200,
            mimetype='application/json'
        )
        return response


