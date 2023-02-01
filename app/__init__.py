from flask import Flask
from flask_cors import CORS
global memcache

webapp = Flask(__name__)
CORS(webapp)
memcache = {}


from app import main







