import xml.dom.minidom
from collections import defaultdict
from datetime import datetime, timedelta

import pytz
from aiohttp import web

from settings import *


def getResponseJSON(code, message, data):
    response_obj = {RESPONSE_CODE: code, RESPONSE_MESSAGE: message}
    if len(data) > 0:
        response_obj[RESPONSE_DATA] = data
    return response_obj


routes = web.RouteTableDef()


@routes.get('/collectors/db')
async def get_db_apss(request):
    """
    Отправка пароля к локальной Базе данных
    Логика:
    1. Машина обращаясь к настройкам службы в реестре, запрашивает пароль к локальной базе данных. Если Пароль получен,
    то формируется строка подключения к локальной БД. Если пароль не подошел, то отправляется статуст службы 4 - пароль
    не правильный. если служба не получила вообще пароля, то отрпавляется статус 5 - не получен пароль со стороны серевера.
    2. при получении статусов 4 и 5 отпраялются сообщения в таблицу messenger, о проблемах с паролем. При этом служба
    продолжает отбиваться на сервер.
    сообщения в мессенжер отправляются однократно при первом обнаружении проблемы с паролем.
    3. пока пароль не получен, ни какие манипуляции с локальной БД невозможны.
    4. Пароль запрашивается однократно, заноситься в оперативную память машины, ни где не храниться.
     После успеха получения пароля, запрашивание пароля прекарщается до рестарта службы.


    :param request:
    :return:
    """
    try:
        logger = request.app['logger']
        # logger.info('Enter to db_pass')
        pool = request.app[POOL_NAME]
        # logger.info('Get pool')
        params = request.rel_url.query
        # logger.info(f'get params {params}')
        if pool is None:
            logger.error('2. No connection to the database')
            return web.json_response(getResponseJSON(5, 'No connection to the database', {}),
                                     status=RESPONSE_STATUS)
        async with pool.acquire() as conn:
            async with conn.transaction():

                db_pass = await conn.fetchval(f'SELECT db_pass FROM polycomm_device WHERE code={params["id"]};')
                if db_pass:
                    logger.info(f'get pass {db_pass}')
                    return web.json_response(getResponseJSON(0, 'Request successfully processed', {'db_pass': db_pass}),
                                             status=RESPONSE_STATUS)
                else:
                    return web.json_response(getResponseJSON(12, 'Dont have any data', {'db_pass': None}),
                                             status=RESPONSE_STATUS)
    except Exception as exc:
        logger.error(f'2a. {exc}')


@routes.get('/collectors/state')
async def chek_state_of_machine(request):
    """
    ЭНд поинт для получения статусов о состоянии срвиса
    :param request: ?id={int}&stateval={int}
    :return:'stateval':"ok"
    +++++    http://develop.db.packandfly.ru:8071/collectors/state?id=3196&stateval=3 ++++++
    """
    try:
        logger = request.app['logger']
        pool = request.app[POOL_NAME]
        params = request.rel_url.query
        # logger.info(f'1. Status of device = {params["id"]} is {params["stateval"]}')
        if pool is None:
            logger.error('2. No connection to the database')
            return web.json_response(getResponseJSON(5, 'No connection to the database', {}),
                                     status=RESPONSE_STATUS)
        async with pool.acquire() as conn:
            async with conn.transaction():
                polycom_dev_id = await conn.fetchval(f'SELECT id FROM polycomm_device WHERE code={params["id"]};')
            select_query = f'SELECT service_status_type FROM timestamps WHERE devicecode=\'{params["id"]}\';'
            async  with conn.transaction():
                 check = await conn.fetchval(select_query)
            if check is not 0:
                if polycom_dev_id:
                    state_val=('1','2','3','4','5')
                    if params["stateval"] in state_val:
                        if params["stateval"] is '1':
                            message = f'Service on machine {params["id"]} is turned on'
                        elif params["stateval"] is '2':
                            message = f'Service on machine {params["id"]} is off'
                        elif params["stateval"] is '3':
                            update_query = f'UPDATE timestamps SET ' \
                                f'lastresponse_service=\'{datetime.now().isoformat(sep=" ")}\', ' \
                                f'service_status_type={params["stateval"]} ' \
                                f'WHERE devicecode=\'{params["id"]}\';'
                            async with conn.transaction():
                                await conn.execute(update_query)
                            return web.json_response(
                                getResponseJSON(0, 'Request successfully processed', {'stateval': "ok"}),
                                status=RESPONSE_STATUS)
                        elif params['stateval'] is '4':
                            if check is not 4:
                                message = f'Service on machine {params["id"]} cannot connect to local DB'
                        elif params['stateval'] is '5':
                            if check is not 5:
                                message = f'The service on machine {params["id"]} did not receive' \
                                    f' a password from the server'

                        update_query = f'UPDATE timestamps SET ' \
                            f'lastresponse_service=\'{datetime.now().isoformat(sep=" ")}\', ' \
                            f'service_status_type={params["stateval"]} ' \
                            f'WHERE devicecode=\'{params["id"]}\';'
                        async with conn.transaction():
                            await conn.execute(update_query)
                        await send_message_to_bot(pool, message, polycom_dev_id, logger)

            else:
                logger.info(f'10a. Disable to send any data from machine id = {params["id"]}')

        return web.json_response(
            getResponseJSON(0, 'Request successfully processed', {'stateval': "ok"}),
            status=RESPONSE_STATUS)

    except Exception as exc:
        logger.error(f'6. Some trouble in check_state procedure: {exc}')


async def send_message_to_bot(pool, message, polycom_dev_id, logger):
    """
    Отправка сообщения в бот
    :param pool:
    :param message: текст сообщения
    :param polycom_dev_id: id из polycomm_device
    :return:
    """
    try:
        insert_query = f'INSERT INTO messenger (device, body) VALUES ({polycom_dev_id}, ' \
            f'\'{message}\') RETURNING messenger_id;'
        async with pool.acquire() as conn:
            async with conn.transaction():
                messenger_id = await conn.fetchval(insert_query)
        if messenger_id:
            logger.info(f'4. {message} was send ')
        else:
            logger.info(f'4.{message} wasm`t send to bot '
                        f'but a message was not sent to the bot')
    except Exception as exc:
        logger.error(f'Problem with send to bot:{exc}')



@routes.get('/collectors/check')
async def check(request):
    """
    проверка статуса машины
    :param request: ?id={int}&status={int}
    :return: {status:True or False}
    http://develop.db.packandfly.ru:8071/collectors/check?id=2812&status=1 ++++++
    """
    try:
        if request.method == "GET":
            pool = request.app[POOL_NAME]
            logger = request.app['logger']
            if pool is None:
                logger.error('8.No connection to the database')
                return web.json_response(getResponseJSON(5, 'No connection to the database', {}),
                                         status=RESPONSE_STATUS)
            params = request.rel_url.query
            async with pool.acquire() as conn:
                select_query = f'SELECT service_status_type FROM timestamps WHERE devicecode=\'{params["id"]}\';'
                async with conn.transaction():
                    check = await conn.fetchval(select_query)
                async with conn.transaction():
                    polycom_dev_id = await conn.fetchval(
                        f'SELECT id FROM polycomm_device WHERE code={params["id"]};')
                if check is 7 and params['status'] is '0':
                    return web.json_response(getResponseJSON(0, 'Request successfully processed', {'status': True}),
                                                 status=RESPONSE_STATUS)
                elif check is not 0 and params['status'] is "1":
                    # logger.info("All valid, ready to recive data")

                    return web.json_response(getResponseJSON(0, 'Request successfully processed', {'status': True}),
                                                 status=RESPONSE_STATUS)

                elif check is not 0 and params['status'] is '0':
                    '''
                    Обновляем статус машины как выключенной... отправляем на машину флаг запрета отправки данных
                    '''
                    update_query = f'UPDATE timestamps SET ' \
                            f'lastresponse_service=\'{datetime.now().isoformat(sep=" ")}\', ' \
                            f'service_status_type=0 ' \
                            f'WHERE devicecode=\'{params["id"]}\';'
                    async with conn.transaction():
                        await conn.execute(update_query)
                    message = f'Disable to send any data from machine {params["id"]}. WARNING: ' \
                              f'Database file spoofing possible!!!'
                    await send_message_to_bot(pool, message, polycom_dev_id,logger)
                    logger.info(f'9. Disable to send any data from machine id = {params["id"]}')
                    return web.json_response(
                           getResponseJSON(3, 'Stop to send any data', {'status': False}),
                           status=RESPONSE_STATUS)
                elif check is 0:
                        logger.info(f'10. Disable to send any data from machine id = {params["id"]}')
                        return web.json_response(getResponseJSON(3, 'Stop to send any data', {'status': False}),
                            status=RESPONSE_STATUS)
    except Exception as exc:
        logger.error(f'11. Some trouble in procedure check response: {exc}')


@routes.get('/collectors')
async def collectors(request):
    '''
    GET-запрос на последний ID в базе по ID машины и таблице
    :param request:?id={int}&tb_name={string}  ?id=2812&tb_name=Suitcase
    :return:{'polycommid': 0, 'totalid': 0, 'partialid': 0}

    ++++++ http://develop.db.packandfly.ru:8071/collectors?id=2812&tb_name=Suitcase ++++++++
    '''
    try:
        if request.method == "GET":
            pool = request.app[POOL_NAME]
            logger = request.app['logger']
            if pool is None:
                logger.error('12. No connection to the database')
                return web.json_response(getResponseJSON(5, 'No connection to the database', {}),
                                         status=RESPONSE_STATUS)
            params = request.rel_url.query

            if params:
                async with pool.acquire() as conn:
                    async with conn.transaction():
                        polycom_dev_id = await conn.fetchval(f'SELECT id FROM polycomm_device WHERE code={params["id"]};')
                if not polycom_dev_id:
                    message = f'Machine with serial number {params["id"]} trying to send data into DB, but ' \
                                    f'it isn`t registred'
                    await send_message_to_bot(pool, message, 0,logger)
                else:
                    last_id = await get_last_ids(polycom_dev_id, params['tb_name'], pool, logger)
                    return web.json_response(
                                    getResponseJSON(0, 'Request successfully processed', dict(last_id)),
                                    status=RESPONSE_STATUS)
            else:
               return web.json_response(getResponseJSON(0, 'All OK, server is redy!!!', {}),
                                status=RESPONSE_STATUS)

    except Exception as ex:
        logger.error('15 Exception from get request to big DB: ' + str(ex))


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
                    await send_data_to_big_db(polycom_dev_id['id'], tb_name, records, request, tz)
    except Exception as ex:
        logger.error(f'19 Exeception from post request to big DB {ex}')


async def get_dev_id_and_tz(pool, logger, data):
    """
    получаем занчения идентификатора машины в БД поликом, ее таймзону и
    """
    try:
        async with pool.acquire() as conn:
            async with conn.transaction():
                polycom_dev_id = await conn.fetchrow(f'SELECT id, city FROM polycomm_device '
                                                     f'WHERE code={data["machineId"]};')
                if not polycom_dev_id:
                    message = f'Machine {data["machineId"]} isn`t registered '
                    async with conn.transaction():
                        await send_message_to_bot(conn, message, 0, logger)
                    logger.warning(f'18. Reciving data from unregistred machine id = {data["machineId"]}')
                    # return web.json_response(getResponseJSON(3, 'Unregistred Machine ID', {}),
                    #                        status=RESPONSE_STATUS)
                else:
                    async with conn.transaction():
                        city_timezone = await conn.fetchval(
                        f'SELECT timezone FROM pnf_city WHERE pnf_city_id = {polycom_dev_id["city"]};')
                    async with conn.transaction():
                        tz = await conn.fetchval(f'SELECT code FROM pnf_timezone WHERE id = {city_timezone};')
                    return polycom_dev_id, tz
    except Exception:
        logger.error(f'{polycom_dev_id["city"]}')


async def send_data_to_big_db(poly_id, tb_name, records, request, timezone):
    logger = request.app['logger']
    pool = request.app[POOL_NAME]
    try:
        device_tz = pytz.timezone(timezone)
        server_tz = pytz.timezone(CURRENT_TIMEZONE)
        delta = server_tz.utcoffset(datetime.now()) - device_tz.utcoffset(datetime.now())
    except Exception as exc:
        logger.error(f'20 Exception from calc delta : {exc}')
    records.sort(key=lambda x: x["ID"])
    if tb_name == 'Suitcase':
        try:
            dict_of_issue_dict = {}
            # logger.info(records)
            for rec in records:
                # logger.info(rec)
                polycomm_id, issue_dict = await insert_suitcase_data(pool, delta, logger, poly_id, rec)

                if issue_dict:
                    dict_of_issue_dict[polycomm_id] = issue_dict
                    for keys_dict in dict_of_issue_dict.keys():
                        for inner_keys in dict_of_issue_dict[keys_dict].keys():
                            await set_polycomm_issue(pool, type_issue=inner_keys, suitcase=keys_dict,
                                                     logger=logger)
            return web.json_response(getResponseJSON(0, 'Request successfully processed',{}),
                                     status=RESPONSE_STATUS)
        except Exception as exc:
            logger.error(f"22 Some trouble detected in insert suitcase procedure: {exc}")

    elif tb_name == 'Allarmi':
        try:
            # logger.error('Alarm enter')
            async with pool.acquire() as conn:
                async with conn.transaction():
                    alarm_types = await conn.fetch(f'SELECT en, polycomm_alarm_type_id FROM polycomm_alarm_type;')
            alarms = dict(alarm_types)
            for rec in records:
                await insert_alarm_data(alarms, pool, delta, logger, poly_id, rec)
        except Exception as exc:
            logger.error(f"25 Some trouble detected in insert alarm procedure: {exc}")


async def insert_alarm_data(alarms, pool, delta, logger, poly_id, rec):
    """
        Формируем строку запроса в таблицу алармов
    """
    try:
        if 'T' in rec['Data']:
            rec['Data'] = rec["Data"].replace('T', ' ')
        localdate = datetime.fromisoformat(rec['Data'])
        moscowdate = localdate + timedelta(seconds=delta.total_seconds())
        if 'Total_Suitcase' in rec.keys():
            insert_query = f'INSERT INTO polycommalarm (id,' \
                f'device,' \
                f'polycommid,' \
                f'date,' \
                f'message,' \
                f'total,' \
                f'alarmtype,' \
                f'localdate) VALUES(1,' \
                f'{poly_id},' \
                f'{rec["ID"]},' \
                f'\'{moscowdate}\',' \
                f'\'{rec["Messaggio"]}\',' \
                f'{rec["Total_Suitcase"]},' \
                f'{alarms[rec["Messaggio"]]}, ' \
                f'\'{localdate}\') RETURNING polycommalarm_id;'
        else:
            # print(al[rec["Messaggio"]])
            insert_query = f'INSERT INTO polycommalarm (id,' \
                f'device,' \
                f'polycommid,' \
                f'date,' \
                f'message,' \
                f'alarmtype,' \
                f'localdate) VALUES (1,' \
                f'{poly_id},' \
                f'{rec["ID"]},' \
                f'\'{moscowdate}\',' \
                f'\'{rec["Messaggio"]}\',' \
                f'\'{alarms[rec["Messaggio"]]}\', ' \
                f'\'{localdate}\') RETURNING polycommalarm_id;'

        async with pool.acquire() as conn:
            async with conn.transaction():
                polycomm_alarm_id = await conn.fetchval(insert_query)

            if polycomm_alarm_id:
                update_query = f'UPDATE polycommalarm SET id={polycomm_alarm_id} where polycommalarm_id={polycomm_alarm_id};'
            # logger.info(update_query)

                async with conn.transaction():
                    await conn.execute(update_query)
                if VERBOSE == 3:
                    logger.info(f'23 Recived and inserted into DB table '
                            f'alarm polycomm_alarm_id= {polycomm_alarm_id} from machne_id = {poly_id}')
                return web.json_response(getResponseJSON(0, 'Request successfully processed',
                                                         {'polycomm_alarm_id': polycomm_alarm_id}),
                                                        status=RESPONSE_STATUS)
            else:
                logger.info(f'24 Alarm wasn`t inserted from machine {poly_id}')
                return web.json_response(getResponseJSON(6, 'Could not add alarm to DB',
                                                     {}), status=RESPONSE_STATUS)

    except Exception:
        logger.exception('trouble2')


async def insert_suitcase_data(pool, delta, logger, poly_id, rec):
    try:
        # logger.info(rec)
        if rec:
            if 'Data_Fine' not in rec.keys():
                insert_query, issue_dict = await create_insert_query_polycomm(pool, delta, logger, poly_id, rec)
            else:
                insert_query, issue_dict = await create_insert_query_packfly(pool, delta, logger, poly_id, rec)
            # logger.info(insert_query)
        polycomm_id = None
        if insert_query:
            async with pool.acquire() as conn:
                async with conn.transaction():
                    polycomm_id = await conn.fetchval(insert_query)
                    if polycomm_id:
                        update_query = f'UPDATE polycomm_suitcase SET id={polycomm_id} where polycom_id={polycomm_id};'
                        await conn.execute(update_query)
                        if VERBOSE == 3:

                            logger.info(
                                f'21 insert polycomsuitcase polycom_id= {polycomm_id} from machine_id = {poly_id}')
            return polycomm_id, issue_dict
        else:
            logger.info('22a Suitcase wasn`t inserted')
            return None, None
    except Exception as exc:
        logger.error(f'trouble:{exc}')


async def get_last_ids(id, name, pool, logger):
    """
        Формирование ответа для поликом машины с последним ID упаковки/аларма
    :param id: Polycomm ID машины
    :param name: название таблицы
    :param conn: объект подключения к базе
    :return: словарь дентификаторов последней записи упаковки/аларма
    """
    try:

            if name == 'Suitcase':
                async with pool.acquire() as conn:
                    async with conn.transaction():
                        last_id = await conn.fetchrow(f'SELECT polycommid, totalid, partialid FROM polycomm_suitcase '
                                                      f'WHERE device =\'{id}\' ORDER BY polycommid DESC LIMIT 1;',timeout=10)
                        if not last_id:
                            last_id = {'polycommid': 0, 'totalid': 0, 'partialid': 0}

            elif name == 'Allarmi':
                async with pool.acquire() as conn:
                    async with conn.transaction():
                        last_id = await conn.fetchrow(f'SELECT polycommid, total FROM polycommalarm '
                                                      f'WHERE device =\'{id}\' ORDER BY polycommid DESC LIMIT 1;',timeout=10)
                        if not last_id:
                            last_id = {'polycommid': 0, 'total': 0}
                        elif not last_id['total']:
                            last_id = {'polycommid': last_id['polycommid']}
            return last_id
    except Exception as exc:
        logger.exception(f"get last ids error: {exc} {id} {name}")


async def create_insert_query_polycomm(pool, delta, logger, poly_id, rec):
    '''
    Формируем строку запроса для вставки упаковки в бльшую БД из локальной БД Polycomm
    '''
    try:
        async with pool.acquire() as conn:
            async with conn.transaction():
                last_id = await conn.fetchrow(f'SELECT polycommid, totalid, partialid FROM polycomm_suitcase '
                                              f'WHERE device =\'{id}\' ORDER BY polycommid DESC LIMIT 1;',timeout=30)
        if not last_id:
            last_id = {'polycommid': 0, 'totalid': 0, 'partialid': 0}
        #last_id = await get_last_ids(poly_id, "Suitcase", pool, logger)


        if rec["ID"]>last_id['polycommid']:
            if not rec["Data_ini"]:
              rec['Data_ini'] = rec['Data']
            if 'T' in rec['Data']:
                rec['Data'] = rec["Data"].replace('T', ' ')
                rec['Data_ini'] = rec["Data_ini"].replace('T', ' ')

            result_pack_type = check_ids_increment(last_id, logger, poly_id, rec)
            start_time = datetime.fromisoformat(rec["Data_ini"])
            # start_time = datetime.strptime(rec["Data_ini"], FORMAT_DATE_TIME)
            end_time = datetime.fromisoformat(rec["Data"])
            # end_time = datetime.strptime(rec["Data"], FORMAT_DATE_TIME)
            moscow_date = end_time + timedelta(seconds=delta.total_seconds())
            moscow_dateini = start_time + timedelta(seconds=delta.total_seconds())
            duration = end_time - start_time
            issue_dict = defaultdict(int)

            if result_pack_type == 777:
                issue_dict['invalidRecordsNumeration'] += 1
                result_pack_type = 1
            elif result_pack_type == 888:
                issue_dict['invalidSingleRecordsNumeration'] += 1
                result_pack_type = 1
            elif result_pack_type == 999:
                issue_dict['invalidDoubleRecordsNumeration'] += 1
                result_pack_type = 1
            min_time, max_time = await get_max_min_duration(pool, logger)
            if duration.seconds < min_time:
                issue_dict['durationBelowLimit'] += 1
            elif duration.seconds > max_time:
                issue_dict['durationOverLimit'] += 1
            insert_query = f'INSERT INTO polycomm_suitcase (packer_error,' \
                    f'device,' \
                    f'device_id,' \
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
                    f'dateini_local) VALUES ({False},' \
                    f'\'{poly_id}\',' \
                    f'{poly_id},' \
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
                    f'{result_pack_type}, {result_pack_type}, {False},' \
                    f'\'{end_time}\', \'{start_time}\') RETURNING polycom_id;'
            return insert_query, issue_dict
        else:
            return None, None
    except Exception as exc:
        logger.exception(f'create_insert_query_polycomm problem {poly_id}: {exc}')


async def create_insert_query_packfly(pool, delta, logger, poly_id, rec):
    """
                Формируем строку запроса для данных из локальной БД Packfly
    :param conn:
    :param delta:
    :param logger:
    :param poly_id:
    :param rec:
    :return:
    """
    try:
        async with pool.acquire() as conn:
            async with conn.transaction():
                last_id = await conn.fetchrow(f'SELECT polycommid, totalid, partialid FROM polycomm_suitcase '
                                              f'WHERE device =\'{id}\' ORDER BY polycommid DESC LIMIT 1;')
        if not last_id:
            last_id = {'polycommid': 0, 'totalid': 0, 'partialid': 0}
        #last_id = await get_last_ids(poly_id, "Suitcase", pool, logger)

        if 'T' in rec['Data_Fine']:
            rec['Data_Fine'] = rec["Data_Fine"].replace('T', ' ')
            rec['Data_ini'] = rec["Data_ini"].replace('T', ' ')
        start_time = datetime.strptime(rec["Data_ini"], FORMAT_DATE_TIME)
        end_time = datetime.strptime(rec["Data_Fine"], FORMAT_DATE_TIME)
        moscow_date = end_time + timedelta(seconds=delta.total_seconds())
        moscow_dateini = start_time + timedelta(seconds=delta.total_seconds())
        duration = end_time - start_time
        issue_dict = defaultdict(int)
        if rec['ID'] - last_id['polycommid'] is not 1:
            issue_dict['invalidRecordsNumeration'] += 1
        min_time, max_time = await get_max_min_duration(pool, logger)
        if duration.seconds < min_time:
            issue_dict['durationBelowLimit'] += 1
        elif duration.seconds > max_time:
            issue_dict['durationOverLimit'] += 1
        if (rec["Ricetta"] is 0):
            insert_query = f'INSERT INTO polycomm_suitcase (' \
                f'packer_error,' \
                f'device,' \
                f'device_id,' \
                f'polycommid,' \
                f'dateini,' \
                f'date,' \
                f'totalid,' \
                f'partialid, ' \
                f'alarmon,' \
                f'outcome,' \
                f'duration, ' \
                f'resolved,' \
                f'local_date,' \
                f'dateini_local) VALUES (' \
                f'{False},' \
                f'\'{poly_id}\',' \
                f'{poly_id},' \
                f'{rec["ID"]},' \
                f'\'{moscow_dateini}\',' \
                f'\'{moscow_date}\',' \
                f'{rec["ID_Totale"]},' \
                f'{rec["ID_Parziale"]},' \
                f'{True if rec["Allarme"] > 0 else False},' \
                f'{rec["Esito"]},' \
                f'{duration.seconds},' \
                f'{False},' \
                f'\'{end_time}\', \'{start_time}\') RETURNING polycom_id;'
        else:
            insert_query = f'INSERT INTO polycomm_suitcase (' \
                f'packer_error,' \
                f'device,' \
                f'device_id,' \
                f'polycommid,' \
                f'dateini,' \
                f'date,' \
                f'totalid,' \
                f'partialid, ' \
                f'alarmon,' \
                f'outcome,' \
                f'duration, ' \
                f'package_type, ' \
                f'package_type_final, ' \
                f'resolved,' \
                f'local_date,' \
                f'dateini_local) VALUES (' \
                f'{False},' \
                f'\'{poly_id}\',' \
                f'{poly_id},' \
                f'{rec["ID"]},' \
                f'\'{moscow_dateini}\',' \
                f'\'{moscow_date}\',' \
                f'{rec["ID_Totale"]},' \
                f'{rec["ID_Parziale"]},' \
                f'{True if rec["Allarme"] > 0 else False},' \
                f'{rec["Esito"]},' \
                f'{duration.seconds},' \
                f'{rec["Ricetta"]}, {rec["Ricetta"]}, {False},' \
                f'\'{end_time}\', \'{start_time}\') RETURNING polycom_id;'
        return insert_query, issue_dict
    except Exception as exc:
        logger.exception(f'25a \'{exc}\'')


def check_ids_increment(last_id, logger, poly_id, rec):
    # {'polycommid': 0, 'totalid': 0, 'partialid': 0}
    pack_type = 1
    if last_id:
        if rec["ID"] - last_id['polycommid'] == 1:
            if rec["ID_Totale"] - last_id['totalid'] == 1 and rec["ID_Parziale"] - last_id['partialid'] == 0:
                pack_type = 1
            elif rec["ID_Parziale"] - last_id['partialid'] == 1 and rec["ID_Totale"] - last_id['totalid'] == 0:
                pack_type = 2
            elif rec["ID_Totale"] - last_id['totalid'] not in range(0, 2):
                pack_type = 888
                logger.info(f'30b total_id currupted counter on device =\'{poly_id}\' ')
            elif rec["ID_Parziale"] - last_id['partialid'] not in range(0, 2):
                pack_type = 999
                logger.info(f'31 partial_id currupted counter device =\'{poly_id}\'')
        elif rec["ID"] - last_id['polycommid'] is not 1:
            pack_type = 777
            logger.info(f'26 last_id wasn`t incremented.Some trouble on device =\'{poly_id}\' {pack_type}')
    else:
        logger.error(f'27 last_id wasn`t incremented. Some trouble on device =\'{poly_id}\'')

    return pack_type


async def get_max_min_duration(pool, logger):
    '''
    Процедура получения граничных значений длительностьей упаковок
    :param conn: подключение к БД
    :param logger: указатель на систему логгирования
    :return:
    '''
    try:
        async with pool.acquire() as conn:
           async with conn.transaction():
                min_time, max_time = await conn.fetchrow(
                    'SELECT suitcase_dur_min_thres, suitcase_dur_max_thres  FROM pnf_config WHERE pnf_config_id=1;', timeout=30)
        if not max_time or not min_time:
            min_time, max_time = 30, 120
        return min_time, max_time
    except Exception as exc:
        logger.error(f'28 Cannot recive data from pnf_config table: {exc}')


async def set_polycomm_issue(pool, type_issue, suitcase, logger):
    """
    Формирование уведомления PoycommIssue
    :param id:
    :param conn:
    :param type_issue:
    :return:
    """
    try:
        # select_query = f'SELECT * FROM polycomm_suitcase WHERE polycom_id={suitcase};'
        # logger.info(f'SELECT * FROM polycomm_suitcase WHERE polycom_id={suitcase};')

        async with pool.acquire() as conn:
            async with conn.transaction():
                last_row = await conn.fetchrow(f'SELECT * FROM polycomm_suitcase WHERE polycom_id={suitcase};')

            if last_row:
                async with conn.transaction():
                    type_issue_id = await conn.fetchval(f'SELECT polycomm_issue_type_id FROM '
                                                        f'polycomm_issue_type WHERE code=\'{type_issue}\' ;')
            insert_query = f'INSERT INTO polycommissue (id,localdate, device, total, suitcase, duration, type, date, callback) ' \
                f'VALUES (22222222,' \
                f'\'{last_row["local_date"]}\',' \
                f'{last_row["device"]},' \
                f'{last_row["totalid"]},' \
                f'{suitcase},' \
                f'{last_row["duration"]},' \
                f'{type_issue_id},' \
                f'\'{last_row["date"]}\',' \
                f'{False})  RETURNING polycommissue_id;'
            # print(insert_query)

            async with conn.transaction():
                result = await conn.fetchval(insert_query)

            return result
    except Exception as exc:
        logger.exception(f'29 Cannot insert issue to data table  from device id={last_row["device"]}: {exc}')


@routes.get('/collectors/last_service')
def download(request):
    try:
        logger = request.app['logger']
        file_n = os.path.join(REPOSITORY, 'PNFService.exe')
        return web.FileResponse(file_n)
    except Exception:
        logger.exception("вот")


@routes.get('/collectors/need_update')
async def needupadte(request):
    """
    Эндпоинт для проверки необходимости обновления файла службы. Так же постит значение версии файла службы
     на конкретной машине. Пости сообщение в бот об обновлении
     :param request:
    :return: True если обновление необходимо, False если нет
    """
    try:
        if request.method == 'GET':
            pool = request.app[POOL_NAME]

            logger = request.app['logger']
            if pool is None:
                logger.error('12. No connection to the database')
                return web.json_response(getResponseJSON(5, 'No connection to the database', {}),
                                         status=RESPONSE_STATUS)

            params = request.rel_url.query
            if params:

                file_n = os.path.join(REPOSITORY, 'PNFService.exe.manifest')
                # logger.info(file_n )
                ver = params['ver']

                # print(ver)
                # ver_file = methods.getFileProperties(file_n)
                if os.path.exists(file_n):
                    # logger.info(f'TRUE')
                    ver_file = read_manifest(file_n)
                else:
                    logger.error(
                        f'34 In REPO is not manifest file')
                    return web.json_response(
                        getResponseJSON(0, 'Need update service', {"need_update": False}), status=RESPONSE_STATUS)

                # print(ver_file['FileVersion'])

                if ver == ver_file:
                    async with pool.acquire() as conn:
                        async with conn.transaction():
                            try:
                                update_query = f'UPDATE polycomm_device SET service_version=\'{ver_file}\'' \
                                    f' where code=\'{params["id"]}\';'
                                # logger.info(update_query)
                                await conn.execute(update_query)
                                if VERBOSE == 3:
                                    # logger.debug('request data: ' + str(data))
                                    logger.info(
                                        f'30 update information of service on machine_id = {params["id"]}')

                            except Exception:
                                logger.exception('31. Trouble with update version of service file')
                    return web.json_response(
                        getResponseJSON(0, 'Need update service', {"need_update": False}),
                        status=RESPONSE_STATUS)
                else:
                    async with pool.acquire() as conn:
                        async with conn.transaction():
                            try:
                                polycom_dev_id = await conn.fetchval(
                                    f'SELECT id FROM polycomm_device WHERE code={params["id"]};')
                                if not polycom_dev_id:
                                    message = f'Machine with serial number {params["id"]} trying to send data into DB, but ' \
                                        f'it isn`t registred'
                                    await send_message_to_bot(conn, message, 0, logger)
                                    logger.info(f'132. {message}')
                                    return web.json_response(
                                        getResponseJSON(0, 'Need update service', {"need_update": False}),
                                        status=RESPONSE_STATUS)
                                else:
                                    message = f'Machine {params["id"]} need to update service file to {ver_file}'
                                    await send_message_to_bot(conn, message, polycom_dev_id,logger)

                            except Exception:
                                logger.exception('33. Cannot send info to messenger')
                    return web.json_response(getResponseJSON(0, 'Need update service', {"need_update": True}),
                                             status=RESPONSE_STATUS)

    except Exception as exc:
        logger.error(f"Update api problem: {exc}")


def read_manifest(manifest):
    doc = xml.dom.minidom.parse(manifest)
    version = doc.getElementsByTagName("assemblyIdentity")
    for f_ver in version:
        if f_ver.getAttribute('name') == 'PNFService':
            return f_ver.getAttribute('version')
