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

os_file_path = "C:/Users/jscst/Flask_Test/"
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

# @webapp.route('/put', methods=['POST'])
# def put():
#     """
#     Upload the key/value pair to the memcache
#     key: string
#     value: string (For images, base64 encoded string)
#     """
#     key = request.form.get('key')
#     value = request.form.get('value')
#     feedback = memcache_global.memcache_put(key, value)
#
#     status_feedback = 200
#     if feedback == "Size too big":
#         status_feedback = 400
#
#     response = webapp.response_class(
#         response=json.dumps(feedback),
#         status=status_feedback,
#         mimetype='application/json'
#     )
#     return response
#

# Pass from Front End Functions
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
    # # Omit DB code
    # allKeys = list(dummyDB.keys())
    # # End of using dummyDB
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


@webapp.route('/getKeys', methods=['GET'])
# return keys list to the web front
def getKeys():
    # query keys stored in database
    # TODO(wkx): db内容查询keys

    # keys = [1, 2, 3]
    keys = db.readAllFileKeys()

    response = webapp.response_class(
        response=json.dumps(keys),
        status=200,
        mimetype='application/json'
    )
    return response


@webapp.route('/allKeyMemcache', methods=['GET'])
def allKeyMemcache():
    """
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


@webapp.route('/deleteKeys', methods=['GET'])
# delete keys from database, return the key list after auditing
def deleteKeys():
    key = request.args.get('key')
    if key:
        logging.info("Delete key: ", key)
        # delete one specified key
        # TODO(wkx): delete key and retrieve
        db.delFileInfo(key)
    else:
        logging.info("Delete all keys")
        # TODO(wkx): delete key and retrieve
        db.delAllFileInfo()

    # keyList = [2, 3]
    keyList = db.readAllFileKeys()

    response = webapp.response_class(
        response=json.dumps(keyList),
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
        # TODO(wkx): get params from database
        cacheConfigs = db.readCacheConfigs()
    elif request.method == 'PUT':
        params = request.args.get('params')
        # TODO(wkx): alter params from database, return altered params
        cacheConfigs = CACHECONFIGS(params['size'], params['policy'])
        db.updCacheConfigs(cacheConfigs)
    params = {
        'size': cacheConfigs.capacity,
        'policy': cacheConfigs.replacementPolicy
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


@webapp.route('/image', methods=['GET', 'POST'])
def imageProcess():
    # get, upload image with key
    key = request.args.get('key')
    if request.method == 'GET':
        # retrieve image
        requestJson = {
            'key': key
        }
        res = requests.post(memcache_host + '/get', params=requestJson)
        if res.status_code == 400:
            # request misses, query db
            res = requests.post(memcache_host + '/getFromDB', params=requestJson)
            content = res.json()
            response = webapp.response_class(
                response=json.dumps(content),
                status=200,
                mimetype='application/json'
            )
            return response
        else:
            print('success image')
            print(res.content)
            content = base64.b64decode(res.content)
            response = webapp.response_class(
                response=json.dumps(bytes.decode(content)),
                status=200,
                mimetype='application/json'
            )
            return response
    elif request.method == 'POST':
        # upload image with key
        # transfer the bytes into dict
        data = eval(bytes.decode(request.data))
        key = data.get('key')
        imageContent = data.get('imageContent')
        requestJson = {
            'key': key,
            'value': base64.b64encode(str(imageContent).encode())
        }
        res = requests.post(memcache_host + '/put', params=requestJson)
        content = res.json()
        response = webapp.response_class(
            response=json.dumps(content),
            status=200,
            mimetype='application/json'
        )
        return response
