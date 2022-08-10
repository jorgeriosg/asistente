
import pymongo
from os import environ
from dotenv import load_dotenv
from os.path import join, dirname


load_dotenv(join(dirname(__file__), '.env'))

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

CONV_INTENTOS = int(environ.get('CONV_INTENTOS', 3))
# mongo
MONGO_URI = environ.get('MONGO_URI')
DATA_BASE = environ.get('DATA_BASE')
INTERACTIONS_COLLECTION = environ.get('INTERACTIONS_COLLECTION')
BENEFICIOS_COLLECTION = environ.get('BENEFICIOS_COLLECTION')
VALORACION_COLLECTION = environ.get('VALORACION_COLLECTION')

# sql
MYSQL_HOST = environ.get('MYSQL_HOST')
MYSQL_DB = environ.get('MYSQL_DB')
MYSQL_USER = environ.get('MYSQL_USER')
MYSQL_PASS = environ.get('MYSQL_PASS')

SG_KEY = environ.get('SG_KEY')
EMAIL_HEROES_ADMIN = environ.get('EMAIL_HEROES_ADMIN')

PORT = 3306

URL_CANALES = environ.get('URL_CANALES')

# --------- logs ---------
DATADOG_API_KEY = environ.get('DATADOG_API_KEY')
DATADOG_APP_KEY = environ.get('DATADOG_APP_KEY')
DATADOG_URL = environ.get('DATADOG_URL')
DATADOG_TAGS = environ.get('DATADOG_TAGS')
DATADOG_SERVICE = environ.get('DATADOG_SERVICE')
DATADOG_SOURCE = environ.get('DATADOG_SOURCE')

ALLOWED_ORIGIN = environ.get('ALLOWED_ORIGIN', '*')
