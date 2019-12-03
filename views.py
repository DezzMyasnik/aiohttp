from aiohttp import web
from settings import *

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
async def collectors(request):
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

                            #last_id = await get_last_tuple_id(polycom_dev_id, params['tb_name'], conn)
                            print(dict(last_id))

                    except Exception as excc:
                        logger.error('Error at conn.fetchval: ' + str(excc))
            return web.Response(text=f"{last_id}")
    except BaseException as ex:
        pass


async def get_last_tuple_id(id, name,conn):
    if name == 'Suitcase':
        last_id = await conn.fetchrow(f'SELECT polycommid, totalid, partialid FROM polycomm_suitcase '
                                      f'WHERE device =\'{id}\' ORDER BY polycommid DESC LIMIT 1;')

    elif name == 'Allarmi':
        pass


    return last_id