import os.path

ROUTE_ROOT = '/'

ROUTE_GET_LAST_ID = '/collectors'

POOL_NAME = 'pool'


FORMAT_DATE = '%d.%m.%Y'
FORMAT_DATE_TIME = '%Y-%m-%d %H:%M:%S'

DB_HOST = '192.168.200.115'
DB_PORT = '5432'
DB_DATABASE = 'control_db'
DB_USER = 'user_ctrl'
DB_PASSWORD = 'Fdeolgn^4dg'

RESPONSE_CODE = 'responseCode'
RESPONSE_MESSAGE = 'responseMessage'
RESPONSE_DATA = 'data'

RESPONSE_STATUS = 200


LOG_FILE_NAME = os.path.dirname(os.path.realpath(__file__)) + '/log/polycommCollector.log'
LOG_FORMAT = '%(asctime)s: %(levelname)s: %(message)s'