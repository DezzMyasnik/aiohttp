from aiohttp import web
from settings import *
from datetime import datetime, timedelta



def getResponseJSON(code, message, data):
    response_obj = {RESPONSE_CODE: code, RESPONSE_MESSAGE: message}
    if len(data) > 0:
        response_obj[RESPONSE_DATA] = data
    return response_obj

routes = web.RouteTableDef()
@routes.get('/')
async def index(request):
    return web.Response(text='Hello Aiohttp!')

@routes.get('/collectors')
@routes.post('/collectors')
async def collectors(request):
    '''
    GET получаем запрос на последний ID в базе по  ID машины и таблице
    :param request:
    :return:
    '''
    try:
        if request.method == "GET":
            pool = request.app[POOL_NAME]

            logger = request.app['logger']
            if pool is None:
                logger.error('No connection to the database')
                return web.json_response(getResponseJSON(5, 'No connection to the database', {}),
                                         status=RESPONSE_STATUS)

            params = request.rel_url.query
            print(params['id'])
            async with pool.acquire() as conn:
                async with conn.transaction():
                    try:
                        polycom_dev_id = await conn.fetchval(f'SELECT id FROM polycomm_device    '
                                                             f'WHERE code={params["id"]};')
                        if polycom_dev_id is None:
                            raise Exception(f'DB don`t contains id= {params["id"]} ')
                        else:
                            last_id =await get_last_tuple_id(polycom_dev_id,params['tb_name'],conn)


                            print(dict(last_id))

                    except Exception as excc:
                        logger.error('Error at conn.fetchval: ' + str(excc))
            return web.Response(text=f"{dict(last_id)}")
        '''
        POST запрос
        '''

        if request.method == 'POST':
            pool = request.app[POOL_NAME]
            logger = request.app['logger']
            if pool is None:
                logger.error('No connection to the database')
                return web.json_response(getResponseJSON(5, 'No connection to the database', {}),
                                         status=RESPONSE_STATUS)
            data = await request.json()

            if data is None:
                logger.warning('Empty JSON in request')
                return web.json_response(getResponseJSON(3, 'Empty JSON in request', {}), status=RESPONSE_STATUS)

            async with pool.acquire() as conn:
                async with conn.transaction():
                    polycom_dev_id = await conn.fetchval(f'SELECT id FROM polycomm_device    '
                                                        f'WHERE code={data["machineId"]};')
                    tb_name = data['dataType']

                    records = data['records']
                    await send_data_to_big_db(polycom_dev_id,tb_name, records,conn)



    except BaseException as ex:
        logger.error('Error at collector: ' + str(ex))





async def send_data_to_big_db(poly_id, tb_name, records, conn):
    if tb_name == 'Suitcase':
        for rec in records:
            '''
            Формируем строку запроса для вставки упаковки в бльшую БД
            '''
            duration = datetime.fromisoformat(rec["Data"]) - datetime.fromisoformat(rec["Data_ini"])
            insert_query = f'INSERT INTO polycomm_suitcase(' \
                f'device,' \
                f'polycommid,' \
                f'dateini,' \
                f'date', \
                f'totalid,' \
                f'partialid, ' \
                f'alarmon,' \
                f'outcome,' \
                f'koweigth,' \
                f'kostop,' \
                f'duration) VALUES (' \
                f'{poly_id},' \
                f'{rec["ID"]},' \
                f'{datetime.fromisoformat(rec["Data_ini"])},' \
                f'{datetime.fromisoformat(rec["Data"])},' \
                f'{rec["ID_Totale"]},' \
                f'{rec["ID_Parziale"]},' \
                f'{rec["Allarme_ON"]},' \
                f'{rec["Esito"]},' \
                f'{rec["KO_Peso"]},' \
                f'{rec["KO_STOP"]},' \
                f'{duration.seconds}) RETURNING polycomm_id;'
            print(insert_query)

    elif tb_name == 'Allarmi':
        pass


async def get_last_tuple_id(id, name,conn):
    """
    Формирование ответа для поликм машины с последним ID упаковки/аларма
    :param id: Polycomm ID машины
    :param name: название таблицы
    :param conn: объект подключения к базе
    :return: словарь дентификаторов последней записи упаковки/аларма
    """
    if name == 'Suitcase':
        last_id = await conn.fetchrow(f'SELECT polycommid, totalid, partialid FROM polycomm_suitcase '
                                      f'WHERE device =\'{id}\' ORDER BY polycommid DESC LIMIT 1;')

    elif name == 'Allarmi':
        last_id = await conn.fetchrow(f'SELECT polycommid, total FROM polycommalarm '
                                      f'WHERE device =\'{id}\' ORDER BY polycommid DESC LIMIT 1;')

    return last_id