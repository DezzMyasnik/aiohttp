from aiohttp import web
from settings import *
from views import *
from collections import defaultdict
from datetime import datetime, timedelta
import pytz

routes = web.RouteTableDef()
@routes.post('/collectors')
async def collector_post(request):
    '''
    POST запрос
    '''
    try:
        if request.method == 'POST':
            pool = request.app[POOL_NAME]
            logger = request.app['logger']
            if pool is None:
                logger.error('16 No connection to the database')
                return web.json_response(getResponseJSON(5, 'No connection to the database', {}),
                                         status=RESPONSE_STATUS)
            data = await request.json()
            if data is None:
                logger.warning('17 Empty JSON in request')
                return web.json_response(getResponseJSON(3, 'Empty JSON in request', {}), status=RESPONSE_STATUS)

            polycom_dev_id, tz = await get_dev_id_and_tz(pool, logger, data)


            if polycom_dev_id['id']:
                tb_name = data['dataType']
                records = data['records']
                if records:
                    pass
    except Exception as ex:
        logger.exception(f'19 Exeception from post request to big DB {ex}')


async def record_process(pool,poly_id, dataType, records, logger):
    try:
        last_id = await get_last_ids(poly_id,dataType,pool,logger)
        if not last_id:
            raise Exception('last_id is empty')

        records.sort(key=lambda x: x["ID"])

        if dataType is "Suitcase":
            for item in records:
                if item['ID'] == last_id[0]+1:
                    last_id[0]=item['ID']
                else:
                    item['issue']

        elif dataType is "Allarmi":
            pass
    except Exception as exc :
        logger.exception(f"Problem in records: {exc}")


async def get_last_ids(id, name, pool, logger):
    """
        Формирование ответа для поликом машины с последним ID упаковки/аларма
    :param id: Polycomm ID машины
    :param name: название таблицы
    :param conn: объект подключения к базе
    :return: словарь идентификаторов последней записи упаковки/аларма
    """
    try:
        async with pool.acquire() as conn:
            async with conn.transaction():
                if name == 'Suitcase':
                    last_id = await conn.fetchrow(f'SELECT polycommid, totalid, partialid FROM polycomm_suitcase '
                                                  f'WHERE device =\'{id}\' ORDER BY polycommid DESC LIMIT 1;')
                    if not last_id:
                        last_id = {'polycommid': 0, 'totalid': 0, 'partialid': 0}

                elif name == 'Allarmi':
                    last_id = await conn.fetchrow(f'SELECT polycommid, total FROM polycommalarm '
                                                  f'WHERE device =\'{id}\' ORDER BY polycommid DESC LIMIT 1;')


                    if not last_id:
                        last_id = {'polycommid': 0, 'total': 0 }
                    elif not last_id['total']:
                        last_id = {'polycommid': last_id['polycommid']}


        return last_id
    except Exception as exc:
        logger.exception(f'Problem in getlastid: {exc}')
