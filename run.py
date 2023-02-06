#!../venv/bin/python
from app import application


application.run('0.0.0.0', 5000, debug=False)
