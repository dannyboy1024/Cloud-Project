from flask import render_template, url_for, request
from app import webapp, memcache_structure, memcache_global, dummyDB
from flask import json
from pathlib import Path
from apscheduler.schedulers.background import BackgroundScheduler
import os
import time
import atexit

os_file_path = "C:/Users/jscst/Flask_Test/"

def print_date_time():
    #We will use this function to implement the timing storage
    print(time.strftime("%A, %d. %B %Y %I:%M:%S %p"))


scheduler = BackgroundScheduler()
scheduler.add_job(func=print_date_time, trigger="interval", seconds=5)
scheduler.start()

# Shut down the scheduler when exiting the app
atexit.register(lambda: scheduler.shutdown())

@webapp.route('/')
def main():
    return render_template("main.html")

@webapp.route('/get',methods=['POST'])
def get():
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


@webapp.route('/put',methods=['POST'])
def put():
    key = request.form.get('key')
    value = request.form.get('value')
    memcache_global.memcache_put(key, value)

    response = webapp.response_class(
        response=json.dumps("OK"),
        status=200,
        mimetype='application/json'
    )
    return response

@webapp.route('/clear',methods=['POST'])
def clear():
    memcache_global.memcache_clear()

    response = webapp.response_class(
        response=json.dumps("OK"),
        status=200,
        mimetype='application/json'
    )
    return response

@webapp.route('/invalidateKey',methods=['POST'])
def invalidateKey():
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

@webapp.route('/refreshConfiguration',methods=['POST'])
def refreshConfiguration():
    # Pending DB code
    size = 50*1024*1024
    mode = "RR"
    # End Pending DB code
    memcache_global.memcache_reconfigure(size, mode)
    response = webapp.response_class(
        response=json.dumps("OK"),
        status=200,
        mimetype='application/json'
    )
    return response

#def schedulingUpdateStat(): 


# Pass from Front End Functions
@webapp.route('/uploadToDB',methods=['POST'])
def uploadToDB():
    key = request.form.get('key')
    value = request.form.get('value')
    #Omit DB code
    dummyDB[key] = "yes"
    #End of using dummyDB
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
    
@webapp.route('/getFromDB',methods=['POST'])
def getFromDB():
    key = request.form.get('key')
    full_file_path = os.path.join(os_file_path, key)
    if not os.path.isfile(full_file_path):
        response = webapp.response_class(
            response=json.dumps("File Not Found"),
            status=400,
            mimetype='application/json'
        )
    else: 
        value=Path(full_file_path).read_text()
        response = webapp.response_class(
            response=json.dumps(value),
            status=200,
            mimetype='application/json'
        )

    return response

@webapp.route('/allKeyDB',methods=['POST'])
def allKeyDB():
    #Omit DB code
    allKeys = list(dummyDB.keys())
    #End of using dummyDB

    response = webapp.response_class(
        response=json.dumps(allKeys),
        status=200,
        mimetype='application/json'
    )

    return response

@webapp.route('/deleteAllFromDB',methods=['POST'])
def deleteAllFromDB():
    #Omit DB code
    allKeys = list(dummyDB.keys())
    #End of using dummyDB
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

@webapp.route('/allKeyMemcache',methods=['GET'])
def allKeyMemcache():
    allKeys = memcache_global.memcache_allkeys()
    response = webapp.response_class(
        response=json.dumps(allKeys),
        status=200,
        mimetype='application/json'
    )
    return response

@webapp.route('/configureMemcache',methods=['POST'])
def configureMemcache():
    size = request.form.get('size')
    mode = request.form.get('mode')
    #omit DB code
    response = webapp.response_class(
        response=json.dumps("OK"),
        status=200,
        mimetype='application/json'
    )
    return response

@webapp.route('/requestCurrentStat',methods=['GET'])
def requestCurrentStat():
    #Omit DB code
    allKeys = list(dummyDB.keys())
    #End of using dummyDB
    response = webapp.response_class(
        response=json.dumps(allKeys),
        status=200,
        mimetype='application/json'
    )
    return response

#For debugging purpose
@webapp.route('/currentConfig',methods=['GET'])
def currentConfig():
    configurationList = memcache_global.current_configuration()
    response = webapp.response_class(
        response=json.dumps(configurationList),
        status=200,
        mimetype='application/json'
    )
    return response