import base64

from flask import render_template, url_for, request
from app import application, memcache_structure, memcache_global, RDBMS, db, CACHECONFIGS, FILEINFO, CACHESTATS
from flask import json
import logging
from pathlib import Path
from apscheduler.schedulers.background import BackgroundScheduler
import os
import time
import atexit
import requests

os_file_path = os.getcwd() + '/fileFolder/'
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


@application.route('/refreshConfiguration', methods=['POST'])
def refreshConfiguration():
    """
    Read from the database and configure memcache setting according to 
    memcache configuration data
    No inputs required
    """

    cacheConfigs = db.readCacheConfigs()
    size = cacheConfigs.capacity
    mode = cacheConfigs.replacementPolicy
    configs = {
        "size" : size,
        "mode" : mode
    }

    memcache_global.memcache_reconfigure(size, mode)
    response = application.response_class(
        response=json.dumps(configs),
        status=200,
        mimetype='application/json'
    )
    return response

@application.route('/cacheOperation', methods=['POST'])
def cacheOperation():
    """
    Turn on or off the operation of memcache
    """
    operation = request.args.get('operation')
    memcache_global.memcache_operating(operation)
    response = application.response_class(
        response=json.dumps("OK"),
        status=200,
        mimetype='application/json'
    )

    return response

@application.route('/uploadToDB', methods=['POST'])
def uploadToDB():
    """
    Upload the key to the database
    Store the value as a file in the local file system, key as filename
    key: string
    value: string (For images, base64 encoded string)
    """
    key = request.args.get('key')
    value = request.args.get('value')
    filename  = request.args.get('name')
    full_file_path = os.path.join(os_file_path, filename)
    if os.path.isfile(full_file_path):
        os.remove(full_file_path)
    with open(full_file_path, 'w') as fp:
        fp.write(value)
    full_file_path = os.path.join(os_file_path, filename)

    if db.readFileInfo(key) == None:
        db.insertFileInfo(FILEINFO(key, full_file_path))
    else:
        db.updFileInfo(FILEINFO(key, full_file_path))

    response = application.response_class(
        response=json.dumps(full_file_path),
        status=200,
        mimetype='application/json'
    )

    return response


@application.route('/getFromLocalFiles', methods=['POST'])
def getFromLocalFiles():
    """
    Fetch the value (file, or image) from the file system given a key
    key: string
    """

    key = request.args.get('key')
    fileInfo = db.readFileInfo(key)
    if fileInfo == None:
        response = application.response_class(
            response=json.dumps("File Not Found"),
            status=400,
            mimetype='application/json'
        )
    else:
        full_file_path = fileInfo.location
        value = Path(full_file_path).read_text()
        response = application.response_class(
            response=json.dumps(value),
            status=200,
            mimetype='application/json'
        )

    return response


@application.route('/allKeyDB', methods=['POST'])
def allKeyDB():
    """
    Display all the keys that stored in the database
    No inputs required
    """

    allKeys = db.readAllFileKeys()

    response = application.response_class(
        response=json.dumps(allKeys),
        status=200,
        mimetype='application/json'
    )

    return response


@application.route('/deleteAllFromDB', methods=['POST'])
def deleteAllFromDB():
    """
    Remove all the key and values (files, images) from the database and filesystem
    No inputs required
    """
    all_full_file_paths = db.readAllFilePaths()
    db.delAllFileInfo()
    for full_file_path in all_full_file_paths:
        if os.path.isfile(full_file_path):
            os.remove(full_file_path)
    response = application.response_class(
        response=json.dumps("OK"),
        status=200,
        mimetype='application/json'
    )

    return response


@application.route('/list_keys', methods=['POST'])
# return keys list to the web front
def getKeys():
    keys = db.readAllFileKeys()
    print("retrieve keys info: ", keys)

    response = application.response_class(
        response=json.dumps({
            "success": "true",
            "keys": keys
        }),
        status=200,
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


@application.route('/delete_all', methods=['POST'])
# delete keys from database, return the key list after auditing
def deleteKeys():
    res = requests.post(memcache_host + '/deleteAllFromDB')
    if res.status_code == 200:
        jsonString = {
            "success": "true"
        }
    else:
        jsonString = 'Fail'
    response = application.response_class(
        response=json.dumps(jsonString),
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
    # omit DB code
    
    db.updCacheConfigs(CACHECONFIGS(size, mode))
    configs = {
        'size' : size,
        'mode' : mode
    }

    response = application.response_class(
        response=json.dumps(configs),
        status=200,
        mimetype='application/json'
    )
    return response


@application.route('/params', methods=['GET', 'PUT'])
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
    params = {
        'size': cacheConfigs.capacity,
        'policy': cacheConfigs.replacementPolicy,
        'operation':  memcache_global.cache_operation
    }
    response = application.response_class(
        response=json.dumps(params),
        status=200,
        mimetype='application/json'
    )
    return response


@application.route('/requestCurrentStat', methods=['GET'])
def requestCurrentStat():
    """
    Display memcache related statistics read from database
    No inputs required
    """

    cacheStats = db.readAllStats()
    stats = {
        'numItems' : [record.numItems for record in cacheStats],
        'totalSize' : [record.totalSize for record in cacheStats],
        'numReqs' : [record.numReqs for record in cacheStats],
        'missRate' : [record.missRate for record in cacheStats],
        'hitRate' : [record.hitRate for record in cacheStats]
    }
    response = application.response_class(
        response=json.dumps(stats),
        status=200,
        mimetype='application/json'
    )
    return response

# For debugging purpose
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

@application.route('/upload', methods=['POST'])
def uploadImage():
    # upload image with key
    # transfer the bytes into dict

    data = request.form
    key = data.get('key')
    imageContent = data.get('file')
    print(eval(imageContent).get('name'))
    requestJson = {
        'key': key,
        'value': base64.b64encode(str(imageContent).encode()),
        'name': eval(imageContent).get('name')
    }
    if memcache_global.cache_operation:
        requests.post(memcache_host + '/put', params=requestJson)
    requests.post(memcache_host + '/uploadToDB', params=requestJson)
    response = application.response_class(
        response=json.dumps({
            "success": "true",
            "key": key
        }),
        status=200,
        mimetype='application/json'
    )
    return response

@application.route('/key/<key_value>', methods=['POST'])
def getImage(key_value):
    # get, upload image with key
    # retrieve image
    requestJson = {
        'key': key_value
    }
    res = None
    if memcache_global.cache_operation:
        res = requests.post(memcache_host + '/get', params=requestJson)
    
    if res == None or res.status_code == 400:

        # cache misses or do not use cache, query db
        print('cache misses or cache not used, query db')
        res = requests.post(memcache_host + '/getFromLocalFiles', params=requestJson)
        if res.status_code == 400:
            content = res.json()
            response = application.response_class(
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
            response = application.response_class(
                response=json.dumps({
                    "success": "true",
                    "key": key_value,
                    "content": bytes.decode(content)
                }),
                status=200,
                mimetype='application/json'
            )
        return response
    else:
        print('cache success')
        content = base64.b64decode(res.content)
        response = application.response_class(
            response=json.dumps(json.dumps({
                    "success": "true",
                    "key": key_value,
                    "content": bytes.decode(content)
                }),),
            status=200,
            mimetype='application/json'
        )
        return response




##################################################################
### Auto testing endpoints (For independent 3rd party testers) ###
##################################################################
from collections import OrderedDict
@application.route('/api/delete_all', methods=['POST'])
def delete_all():
    all_full_file_paths = db.readAllFilePaths()
    db.delAllFileInfo()
    for full_file_path in all_full_file_paths:
        if os.path.isfile(full_file_path):
            os.remove(full_file_path)
    
    resp = {
        "success" : "true"
    }
    response = application.response_class(
        response=json.dumps(resp),
        status=200,
        mimetype='application/json'
    )
    return response

@application.route('/api/upload', methods=['POST'])
def upload():
    key = request.form.get('key')
    image = request.files.get('file')
    imageBytes = image.read()
    encodedImage = base64.b64encode(str(imageBytes).encode())
    requestJson = {
        'key': key,
        'value': encodedImage
    }
    if memcache_global.cache_operation:
        requests.post(memcache_host + '/put', params=requestJson)
    
    full_file_path = os.path.join(os_file_path, image.filename)
    print(full_file_path)
    if os.path.isfile(full_file_path):
        os.remove(full_file_path)
    with open(full_file_path, 'wb') as fp:
        fp.write(encodedImage)

    if db.readFileInfo(key) == None:
        db.insertFileInfo(FILEINFO(key, full_file_path))
    else:
        db.updFileInfo(FILEINFO(key, full_file_path))

    resp = OrderedDict([("success", "true"), ("key", [key])])
    response = application.response_class(
        response=json.dumps(resp),
        status=200,
        mimetype='application/json'
    )
    return response

@application.route('/api/list_keys', methods=['POST'])
def retrieveAll():
    keys = db.readAllFileKeys()
    resp = OrderedDict()
    resp["success"] = "true"
    resp["keys"] = keys
    response = application.response_class(
        response=json.dumps(resp),
        status=200,
        mimetype='application/json'
    )
    return response

@application.route('/api/key/<key_value>', methods=['POST'])
def retrieve(key_value):

    # retrieve image
    requestJson = {
        'key': key_value
    }
    res = None
    if memcache_global.cache_operation:
        res = requests.post(memcache_host + '/get', params=requestJson)
    
    if res == None or res.status_code == 400:
        # cache misses or do not use cache, query db
        print('cache misses or cache not used, query db')
        res = requests.post(memcache_host + '/getFromLocalFiles', params=requestJson)
        if res.status_code == 400:
            resp = OrderedDict()
            resp["success"] = "false"
            resp["error"] = {
                "code": 400,
                "message": "Target file is not found because the given key is not found in database."
            }
            response = application.response_class(
                response=json.dumps(resp),
                status=200,
                mimetype='application/json'
            )
        else:
            content = base64.b64decode(res.content)
            resp = OrderedDict()
            resp["success"] = "true"
            resp["key"] = [key_value]
            resp["content"] = bytes.decode(content)
            response = application.response_class(
                response=json.dumps(resp),
                status=200,
                mimetype='application/json'
            )
        return response
    else:
        print('cache success')
        content = base64.b64decode(res.content)
        resp = OrderedDict()
        resp["success"] = "true"
        resp["key"] = [key_value]
        resp["content"] = bytes.decode(content)
        response = application.response_class(
            response=json.dumps(resp),
            status=200,
            mimetype='application/json'
        )
        return response
