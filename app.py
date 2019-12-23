import argparse
import logging
from logging.handlers import RotatingFileHandler

import asyncpg

from views import *


def make_app(logger):
    app = web.Application()
    # setup_routes(app)
    app.on_startup.append(on_start)
    app.on_shutdown.append(on_shutdown)
    app['logger'] = logger
    app.add_routes(routes)

    return app


async def on_start(app):
    try:
        app[POOL_NAME] = await asyncpg.create_pool(host=DB_HOST, port=DB_PORT, user=DB_USER,
                                                   password=DB_PASSWORD, database=DB_DATABASE)
        # app[POOL_RED_NAME] = await asyncpg.create_pool(host=DB_HOST_RED, port=DB_PORT_RED, user=DB_USER_RED,
        #                                       password= DB_PASSWORD_RED, database=DB_DATABASE_RED)
    except Exception as ex:
        print('Error connecting to the database! Message: ' + str(ex))


async def on_shutdown(app):
    try:
        await app[POOL_NAME].close()
        # await app[POOL_RED_NAME].close()
    except Exception as exc:
        print('Error when disconnecting from the database! Message: ' + str(exc))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='API Server. Polycomm collector\n')
    parser.add_argument('host', nargs='?', default='127.0.0.1', help='IP adress of api server')
    parser.add_argument('-p', '--port', nargs='?', default='8080', help='listening port')
    args = parser.parse_args()

    formatter = logging.Formatter(LOG_FORMAT)
    formatter.datefmt = FORMAT_DATE_TIME
    logger = logging.getLogger("Event log of polycommCollector module")
    logger.setLevel(logging.DEBUG)
    handler = RotatingFileHandler(LOG_FILE_NAME, mode='a', maxBytes=10000000, backupCount=30)  # 10Mb
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    web.run_app(make_app(logger), host=args.host, port=args.port)
