from flask import render_template, url_for, request
from app import webapp, memcache_structure, memcache_global, dummyDB
from flask import json
import logging
from pathlib import Path
from apscheduler.schedulers.background import BackgroundScheduler
import os
import time
import atexit

os_file_path = "C:/Users/jscst/Flask_Test/"


def print_date_time():
    # We will use this function to implement the timing storage
    print(time.strftime("%A, %d. %B %Y %I:%M:%S %p"))


scheduler = BackgroundScheduler()
scheduler.add_job(func=print_date_time, trigger="interval", seconds=5)
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
    key = request.form.get('key')
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
    key = request.form.get('key')
    value = request.form.get('value')
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
    # Pending DB code
    size = 50 * 1024 * 1024
    mode = "RR"
    # End Pending DB code
    memcache_global.memcache_reconfigure(size, mode)
    response = webapp.response_class(
        response=json.dumps("OK"),
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
    key = request.form.get('key')
    value = request.form.get('value')
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


# Pass from Front End Functions
@webapp.route('/uploadToDB', methods=['POST'])
def uploadToDB():
    """
    Upload the key to the database
    Store the value as a file in the local file system, key as filename
    key: string
    value: string (For images, base64 encoded string)
    """
    key = request.form.get('key')
    value = request.form.get('value')
    # Omit DB code
    dummyDB[key] = "yes"
    # End of using dummyDB
    full_file_path = os.path.join(os_file_path, key)
    if os.path.isfile(full_file_path):
        os.remove(full_file_path)
    with open(full_file_path, 'w') as fp:
        fp.write(value)

    response = webapp.response_class(
        response=json.dumps("OK"),
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
    key = request.form.get('key')
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
    # Omit DB code
    allKeys = list(dummyDB.keys())
    # End of using dummyDB

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
    # Omit DB code
    allKeys = list(dummyDB.keys())
    # End of using dummyDB
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
    keys = [1, 2, 3]
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
        pass
    else:
        logging.info("Delete all keys")
        # TODO(wkx): delete key and retrieve
        pass
    keyList = [2, 3]
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
    size = request.form.get('size')
    mode = request.form.get('mode')
    # omit DB code
    response = webapp.response_class(
        response=json.dumps("OK"),
        status=200,
        mimetype='application/json'
    )
    return response


@webapp.route('/params', methods=['GET', 'PUT'])
# return mem-cache configuration params
def getParams():
    if request.method == 'GET':
        # TODO(wkx): get params from database
        pass
    elif request.method == 'PUT':
        params = request.args.get('params')
        # TODO(wkx): alter params from database, return altered params
        pass
    params = {
        'policy': '1',
        'size': 24
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
    # Omit DB code
    allKeys = list(dummyDB.keys())
    # End of using dummyDB
    response = webapp.response_class(
        response=json.dumps(allKeys),
        status=200,
        mimetype='application/json'
    )
    return response


# For debugging purpose
@webapp.route('/currentConfig', methods=['GET'])
def currentConfig():
    """
    Display memcache related statistics tracked by memcache locally
    No inputs required
    """
    configurationList = memcache_global.current_configuration()
    response = webapp.response_class(
        response=json.dumps(configurationList),
        status=200,
        mimetype='application/json'
    )
    return response
