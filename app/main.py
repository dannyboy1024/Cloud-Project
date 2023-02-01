from flask import render_template, url_for, request
from app import webapp, memcache
from flask import json
import logging


@webapp.route('/')
def main():
    return render_template("main.html")


@webapp.route('/get', methods=['POST'])
def get():
    key = request.form.get('key')

    if key in memcache:
        value = memcache[key]
        response = webapp.response_class(
            response=json.dumps(value),
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


@webapp.route('/put', methods=['POST'])
def put():
    key = request.form.get('key')
    value = request.form.get('value')
    memcache[key] = value

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
