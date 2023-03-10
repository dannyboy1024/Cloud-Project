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
    size = int(request.args.get('size'))
    feedback = memcache_global.memcache_put(key, value, size)

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

@application.route('/getCurrentSize', methods=['GET'])
def getCurrentSize():
    """
    Remove all contents in the memcache
    No inputs required
    """
    currentSize = memcache_global.current_size
    response = application.response_class(
        response=json.dumps(currentSize),
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
    resp = {
        "success" : "true", 
        "all_keys": allKeys
    }
    response = application.response_class(
        response=json.dumps(resp),
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
    if "size" in request.args:
        size = int(request.args.get('size'))
    else:
        size = memcache_global.memcache_size / (1024*1024)
    if "mode" in request.args:
        mode = request.args.get('mode')
    else:
        mode = memcache_global.memcache_mode
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
