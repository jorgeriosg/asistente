#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
from os import environ
from dotenv import load_dotenv
from os.path import join, dirname
import pymongo
import socket
import traceback
import requests
import json
from flask import jsonify
from cognitiva_xss_3.xss3 import XssClean
load_dotenv(join(dirname(__file__), 'mods/.env'))

SERVER_PATH = environ.get('SERVER_PATH')
MAX_ERROR = int(environ.get('MAX_ERROR'))

# conversation credentials
CONV_USER = environ.get('CONV_USER')
CONV_PASS = environ.get('CONV_PASS')
CONV_VERSION = environ.get('CONV_VERSION')
CHITCHAT_CONV_USER = environ.get('CHITCHAT_CONV_USER')
CHITCHAT_CONV_PASS = environ.get('CHITCHAT_CONV_PASS')
# conversation workspaces
WORKSPACE = environ.get('WORKSPACE')
CHITCHAT_WORKSPACE = environ.get('CHITCHAT_WORKSPACE')
ATOMICO_WORKSPACE = environ.get('ATOMICO_WORKSPACE')

CONV_INTENTOS = os.getenv('CONV_INTENTOS', 3)
# mongo
MONGO_URI = environ.get('MONGO_URI')
DATA_BASE = environ.get('DATA_BASE')
mongo = pymongo.MongoClient(MONGO_URI)
db = mongo[DATA_BASE]
interactions = db[environ.get('INTERACTIONS_COLLECTION')]
beneficios = db[environ.get('BENEFICIOS_COLLECTION')]
valoracion = db[environ.get('VALORACION_COLLECTION')]

# sql
MySQL_host = environ.get('MYSQL_HOST')
DB_name = environ.get('MYSQL_DB')
MySQL_user = environ.get('MYSQL_USER')
MySQL_pass = environ.get('MYSQL_PASS')

SG_KEY = environ.get('SG_KEY')
EMAIL_HEROES_ADMIN = environ.get('EMAIL_HEROES_ADMIN')

Port = 3306

# --------- logs ---------
DATADOG_API_KEY = environ.get('DATADOG_API_KEY')
DATADOG_APP_KEY = environ.get('DATADOG_APP_KEY')
DATADOG_URL = environ.get('DATADOG_URL')
DATADOG_TAGS = environ.get('DATADOG_TAGS')
DATADOG_SERVICE = environ.get('DATADOG_SERVICE')
DATADOG_SOURCE = environ.get('DATADOG_SOURCE')

ALLOWED_ORIGIN = os.getenv('ALLOWED_ORIGIN', '*')


def datadog_log(function, glosa=''):

    err = "Hay un error en funcion "
    err += function + ' ' + glosa
    # LOG.error(err)
    try:
        url = DATADOG_URL + DATADOG_API_KEY
        headers = {'Content-Type': 'application/json'}

        try:
            hostname = socket.gethostbyname(socket.gethostname())
        except Exception as e:
            hostname = 'Local'
        body = {
            'message': err + ': ' + traceback.format_exc(),
            'cid': '',
            'hostname': hostname,
            'ddtags': DATADOG_TAGS,
            'service': DATADOG_SERVICE,
            'ddsource': DATADOG_SOURCE,
            'status_code': 500,
            'severity': "ERROR"
        }
        requests.post(url, headers=headers,
                      data=json.dumps(body), timeout=2)
    except Exception as e:
        print(str(e))
        pass


def xss2_func(input_text):
    # se mantiene xss2 para no tener que cambiar en muchas partes pero se usa xss3
    if input_text is None or not(isinstance(input_text, str)):
        return input_text
    return XssClean.sanitizeHtml(input_text).strip()
