from aiohttp import web
from settings import *
from datetime import datetime, timedelta,tzinfo
from collections import defaultdict
import pytz

def getResponseJSON(code, message, data):
    response_obj = {RESPONSE_CODE: code, RESPONSE_MESSAGE: message}
    if len(data) > 0:
        response_obj[RESPONSE_DATA] = data
    return response_obj

routes = web.RouteTableDef()

async def chek_state_of_machine(request):
    """
    ЭНд поинт для получения статусов о состоянии срвиса
    :param request:
    :return:
    """
    logger = request.app['logger']
    params = request.rel_url.query
    pass



@routes.get('/collectors/check')
async def check(request):
    """
    проверка статтуса машины
    :param request:
    :return:
    """
    try:
        if request.method == "GET":
            pool = request.app[POOL_NAME]
            logger = request.app['logger']
            if pool is None:
                logger.error('No connection to the database')
                return web.json_response(getResponseJSON(5, 'No connection to the database', {}),
                                         status=RESPONSE_STATUS)
            params = request.rel_url.query
            async with pool.acquire() as conn:
                async  with conn.transaction():

                        check = await conn.fetchval(f'SELECT enabled FROM polycomm_device    '
                                                             f'WHERE code={params["id"] } ;')
                        if check == True and params['status'] is "1":
                            logger.info("All valid, ready to recive data")
                            print(f'1 All valid, ready to recive data')
                            return web.json_response(getResponseJSON(0, 'Request successfully processed', {'status': True}),
                                                    status=RESPONSE_STATUS)

                        elif check == True and params['status'] is '0':
                            '''
                            Обновляем статус машины как выключенной... отправляем на амшину флаг запрета отправки данных
                            '''
                            await conn.execute(f'UPDATE polycomm_device SET enabled=false WHERE code={params["id"]};')
                            logger.error(f'Disable to send any data from machine id = {params["id"]}')
                            print(f'Disable to send any data from machine id = {params["id"]})')
                            return web.json_response(
                                getResponseJSON(3, 'Stop to send any data', {'status': False}),
                                status=RESPONSE_STATUS)

                        elif check == False:
                            logger.error(f'Disable to send any data from machine id = {params["id"]}')
                            return web.json_response(
                                getResponseJSON(3, 'Stop to send any data', {'status': False}),
                                status=RESPONSE_STATUS)


    except Exception as exc:
        logger.error('Исключение при обрабтке запросов check: ' + str(exc))

@routes.get('/collectors')
@routes.post('/collectors')
async def collectors(request):
    '''
    GET-запрос на последний ID в базе по ID машины и таблице
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
            print(params)
            async with pool.acquire() as conn:
                async with conn.transaction():
                    try:
                        polycom_dev_id = await conn.fetchval(f'SELECT id FROM polycomm_device    '
                                                             f'WHERE code={params["id"]};')
                        if polycom_dev_id is None:
                            '''
                            !!!!сделать уведомление в дашборд о незарегистрированной машине
                            '''
                            logger.error(f'Machine with serial number {params["id"]} trying to send data into DB')
                            return web.json_response(
                                getResponseJSON(6, 'The Machine is not registered!!!', {}),
                                status=RESPONSE_STATUS)
                        else:
                            last_id = await get_last_ids(polycom_dev_id, params['tb_name'], conn)
                            return web.json_response(
                                getResponseJSON(0, 'Request successfully processed', dict(last_id)),
                                status=RESPONSE_STATUS)
                    except Exception as excc:
                        logger.error('Error at conn.fetchval at collector function: ' + str(excc))

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
            print(data)
            if data is None:
                logger.warning('Empty JSON in request')
                return web.json_response(getResponseJSON(3, 'Empty JSON in request', {}), status=RESPONSE_STATUS)

            async with pool.acquire() as conn:
                async with conn.transaction():
                    polycom_dev_id = await conn.fetchrow(f'SELECT id, city FROM polycomm_device    '
                                                         f'WHERE code={data["machineId"]};')
                    city_timezone = await conn.fetchval(f'SELECT timezone FROM pnf_city WHERE pnf_city_id = {polycom_dev_id["city"]};')

                    tz = await conn.fetchval(f'SELECT code FROM pnf_timezone WHERE id = {city_timezone};')
                    if polycom_dev_id['id'] is not None:
                        tb_name = data['dataType']

                        records = data['records']

                        if records:
                            resp = await send_data_to_big_db(polycom_dev_id['id'], tb_name, records, conn, request, tz)
                            print(resp)
                            return resp
                    else:
                        '''
                         !!!!сделать уведомление в дашборд о незарегистрированной машине
                        '''
                        logger.warning(f'Reciving data from unregistred machine id = {data["machineId"]}')
                        return web.json_response(getResponseJSON(3, 'Unregistred Machine ID', {}),
                                                 status=RESPONSE_STATUS)



    except BaseException as ex:
        logger.error('Exeception from operate request from suitcase and alarm collector: ' + str(ex))





async def send_data_to_big_db(poly_id, tb_name, records, conn, request, timezone):
    logger = request.app['logger']


    try:
        device_tz = pytz.timezone(timezone)
        server_tz = pytz.timezone(CURRENT_TIMEZONE)
        delta = server_tz.utcoffset(datetime.now()) - device_tz.utcoffset(datetime.now())
        print(delta.total_seconds())
        records.sort(key=lambda x: x["ID"])
        if tb_name == 'Suitcase':

            for rec in records:
                '''
                 Формируем строку запроса для вставки упаковки в бльшую БД
                '''
                pack_type = 1

                last_id = await conn.fetchrow(f'SELECT polycommid, totalid, partialid FROM polycomm_suitcase '
                                            f'WHERE device =\'{poly_id}\' ORDER BY polycommid DESC LIMIT 1;')
                if last_id is not None:
                    if rec['ID'] == last_id[0]+1:
                        if rec["ID_Totale"] == last_id[1]+1:
                            pack_type = 1

                        elif rec["ID_Parziale"] == last_id[2]+1:
                            pack_type = 2
                        else:
                            logger.error('total_id or partial_id don`t incremented')
                            #return web.json_response(getResponseJSON(6, 'total_id or partal_id don`t incremented',
                            #                                         {}), status=RESPONSE_STATUS)
                    else:
                        logger.error( f'last_id wasn`t incremented.Some trouble on machine =  {poly_id}')
                        return web.json_response(getResponseJSON(6, 'last_id wasn`t incremented',
                                                                 {}), status=RESPONSE_STATUS)
                else:
                    logger.info(f'First time add from machine id={poly_id}')


                if 'T' in rec['Data']:
                    rec['Data'] = rec["Data"].replace('T',' ')
                    rec['Data_ini'] = rec["Data_ini"].replace('T', ' ')



                start_time = datetime.fromisoformat(rec["Data_ini"])
                end_time = datetime.fromisoformat(rec["Data"])
                if device_tz is not server_tz:

                    moscow_date = end_time + timedelta(seconds=delta.total_seconds())
                    moscow_dateini = start_time + timedelta(seconds=delta.total_seconds())
                else:
                    moscow_date = end_time
                    moscow_dateini = start_time
                duration =  end_time - start_time


                insert_query = f'INSERT INTO polycomm_suitcase (' \
                    f'device,' \
                    f'polycommid,' \
                    f'dateini,' \
                    f'date,' \
                    f'totalid,' \
                    f'partialid, ' \
                    f'alarmon,' \
                    f'outcome,' \
                    f'koweight,' \
                    f'kostop,' \
                    f'duration, ' \
                    f'package_type, ' \
                    f'package_type_final, ' \
                    f'resolved,' \
                    f'local_date,' \
                    f'dateini_local) VALUES (' \
                    f'\'{poly_id}\',' \
                    f'{rec["ID"]},' \
                    f'\'{moscow_dateini}\',' \
                    f'\'{moscow_date}\',' \
                    f'{rec["ID_Totale"]},' \
                    f'{rec["ID_Parziale"]},' \
                    f'{rec["Allarme_ON"]},' \
                    f'{rec["Esito"]},' \
                    f'{rec["KO_Peso"]},' \
                    f'{rec["KO_STOP"]},' \
                    f'{duration.seconds},' \
                    f'{pack_type}, {pack_type}, {False},' \
                    f'\'{end_time}\', \'{start_time}\') RETURNING polycom_id;'
                    #
                polycomm_id = await conn.fetchval(insert_query)



                if polycomm_id is not None:
                    if VERBOSE == 3:
                        # logger.debug('request data: ' + str(data))
                        logger.info(
                            f'Recived and inserted into DB table polycomsuitcase polycomm_id= {polycomm_id} from machine_id = {poly_id}')

                    #return web.json_response(
                    #    getResponseJSON(0, 'Request successfully processed', {'polycomm_id': polycomm_id}),
                    #    status=RESPONSE_STATUS)
                else:
                    logger.error('Suitcase wasn`t inserted')
                    return web.json_response(getResponseJSON(6, 'Could not add suitcase to DB',
                                                             {}), status=RESPONSE_STATUS)
        elif tb_name == 'Allarmi':
            alarm_types = await conn.fetch(f'SELECT en,polycomm_alarm_type_id FROM polycomm_alarm_type;')
            alarms = dict(alarm_types)
            for rec in records:
                """
                Формируем строку запроса в таблицу алармов
                """
                if 'T' in rec['Data']:
                    rec['Data'] = rec["Data"].replace('T', ' ')
                localdate = datetime.fromisoformat(rec["Data"])
                moscowdate = localdate + timedelta(seconds=delta.total_seconds())
                insert_query = f'INSERT INTO polycommalarm(' \
                    f'device,' \
                    f'polycomid,' \
                    f'date,' \
                    f'message,' \
                    f'new,' \
                    f'total,' \
                    f'alarmtype,' \
                    f'localdate) VALUES(' \
                    f'\'{poly_id}\',' \
                    f'{rec["ID"]},' \
                    f'\'{moscowdate}\',' \
                    f'{rec["Messaggio"]},' \
                    f'{rec["New"]},' \
                    f'{rec["Total_Suitcase"]},' \
                    f'{alarms[rec["Messaggio"]]}, ' \
                    f'\'{localdate}\') RETURNING polycommalarm_id;'

                polycomm_alarm_id = await conn.fetchval(insert_query)
                if polycomm_alarm_id is not None:
                    if VERBOSE == 3:
                        # logger.debug('request data: ' + str(data))
                        logger.info(
                            f'Recived and inserted into DB table alarm polycomm_alarm_id= {polycomm_alarm_id} from machne_id = {poly_id}')

                    return web.json_response(
                        getResponseJSON(0, 'Request successfully processed', {'polycomm_alarm_id': polycomm_alarm_id}),
                        status=RESPONSE_STATUS)
                else:
                    logger.error(f'Alarm wasn`t inserted from machine {poly_id}')
                    return web.json_response(getResponseJSON(6, 'Could not add alarm to DB',
                                                             {}), status=RESPONSE_STATUS)

    except Exception as exc:
        print(exc)
        logger.error(f"Some trouble detected in insert procedure: {exc}")



async def get_last_ids(id, name, conn):
    """
    Формирование ответа для поликом машины с последним ID упаковки/аларма
    :param id: Polycomm ID машины
    :param name: название таблицы
    :param conn: объект подключения к базе
    :return: словарь дентификаторов последней записи упаковки/аларма
    """
    if name == 'Suitcase':
        last_id = await conn.fetchrow(f'SELECT polycommid, totalid, partialid FROM polycomm_suitcase '
                                      f'WHERE device =\'{id}\' ORDER BY polycommid DESC LIMIT 1;')
        if not last_id:
            last_id = {'polycommid': 0, 'totalid': 0, 'partialid': 0}

    elif name == 'Allarmi':
        last_id = await conn.fetchrow(f'SELECT polycommid, total FROM polycommalarm '
                                      f'WHERE device =\'{id}\' ORDER BY polycommid DESC LIMIT 1;')
        if not last_id:
            last_id = {'polycommid':0, 'total':0}



    return last_id