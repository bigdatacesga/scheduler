import os

basedir = os.path.abspath(os.path.dirname(__file__))

DEBUG = False
IGNORE_AUTH = True
SECRET_KEY = 'admin'
MESOS_MASTER = 'mesosmaster.service.int.cesga.es:5050'
