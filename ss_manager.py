import sys
import datetime
import logging
import uuid
from user_manage import Manager
from bottle import route, request, run

FAILED = "Failed: "
OK = "OK"

logger = logging.getLogger('ss_manager')
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setLevel(logging.WARNING)
#fh = logging.FileHandler('/var/log/ss_manager.log')
fh = logging.FileHandler('ss_manager.log')
fh.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(module)s.%(funcName)s [%(levelname)s]: %(message)s')
ch.setFormatter(formatter)
fh.setFormatter(formatter)
logger.addHandler(ch)
logger.addHandler(fh)

manager = None
online_admin = {}


def verify_login(uid):
    for iter in online_admin.items():
        if uid == iter[1]:
            return iter[0]
    return None


@route('/add_admin')
def add_admin():
    username = request.query.username
    pwd = request.query.pwd

    if username is None or pwd is None:
        return FAILED + "Not enough params"

    result = manager.add_admin(username, pwd)
    if result is Manager.ErrType.OK:
        return OK
    else:
        return FAILED + str(result)


@route('/login')
def login():
    username = request.query.username
    pwd = request.query.pwd

    if username is None or pwd is None:
        return FAILED + "Not enough params"

    result = manager.admin_login(username, pwd)
    if result is Manager.ErrType.OK:
        uid = str(uuid.uuid3(uuid.NAMESPACE_OID, username + datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
        online_admin[username] = uid
        return uid
    else:
        return FAILED + str(result)


@route('/add_user')
def add_user():
    uid = request.query.uid
    pwd = request.query.pwd
    expire_time = request.query.expire_time
    limit = request.query.limit or -1
    used = request.query.used or 0
    is_tmp = request.query.is_tmp or 0
    nickname = request.query.nickname or ""
    

    if uid is None or pwd is None or expire_time is None:
        return FAILED + "Not enough params"

    admin = verify_login(uid)
    if admin is None:
        return FAILED + "Wrong login"

    try:
        expire_time = float(expire_time)
        limit = int(limit)
        used = int(used)
        is_tmp = int(is_tmp)
    except ValueError:
        return FAILED + "Wrong param type"

    port = manager.add_user(admin, pwd, datetime.datetime.fromtimestamp(expire_time), limit, used, is_tmp, nickname)

    if port is Manager.ErrType.alloc_port_failed:
        return FAILED + str(port)

    return str(port)


@route('/del_user')
def del_user():
    uid = request.query.uid
    port = request.query.port

    if uid is None or port is None:
        return FAILED + "Not enough params"

    admin = verify_login(uid)
    if admin is None:
        return FAILED + "Wrong login"

    try:
        port = int(port)
    except ValueError:
        return FAILED + "Wrong param type"

    result = manager.del_user(admin, port)
    if result is Manager.ErrType.OK:
        return OK
    else:
        return FAILED + str(result)


@route('/start_user')
def start_user():
    uid = request.query.uid
    port = request.query.port

    if uid is None or port is None:
        return FAILED + "Not enough params"

    admin = verify_login(uid)
    if admin is None:
        return FAILED + "Wrong login"

    try:
        port = int(port)
    except ValueError:
        return FAILED + "Wrong param type"

    result = manager.start_user(admin, port)
    if result is Manager.ErrType.OK:
        return OK
    else:
        return FAILED + str(result)


@route('/stop_user')
def stop_user():
    uid = request.query.uid
    port = request.query.port

    if uid is None or port is None:
        return FAILED + "Not enough params"

    admin = verify_login(uid)
    if admin is None:
        return FAILED + "Wrong login"

    try:
        port = int(port)
    except ValueError:
        return FAILED + "Wrong param type"

    result = manager.stop_user(admin, port)
    if result is Manager.ErrType.OK:
        return OK
    else:
        return FAILED + str(result)


@route('/enable_user')
def enable_user():
    uid = request.query.uid
    port = request.query.port

    if uid is None or port is None:
        return FAILED + "Not enough params"

    admin = verify_login(uid)
    if admin is None:
        return FAILED + "Wrong login"

    try:
        port = int(port)
    except ValueError:
        return FAILED + "Wrong param type"

    result = manager.enable_user(admin, port)
    if result is Manager.ErrType.OK:
        return OK
    else:
        return FAILED + str(result)


@route('/disable_user')
def disable_user():
    uid = request.query.uid
    port = request.query.port

    if uid is None or port is None:
        return FAILED + "Not enough params"

    admin = verify_login(uid)
    if admin is None:
        return FAILED + "Wrong login"

    try:
        port = int(port)
    except ValueError:
        return FAILED + "Wrong param type"

    result = manager.disable_user(admin, port)
    if result is Manager.ErrType.OK:
        return OK
    else:
        return FAILED + str(result)


@route('/change_user_used')
def change_user_used():
    uid = request.query.uid
    port = request.query.port
    used = request.query.used

    if uid is None or port is None or used is None:
        return FAILED + "Not enough params"

    admin = verify_login(uid)
    if admin is None:
        return FAILED + "Wrong login"

    try:
        port = int(port)
        used = int(used)
    except ValueError:
        return FAILED + "Wrong param type"

    result = manager.update_user_used(admin, port, used)
    if result is Manager.ErrType.OK:
        return OK
    else:
        return FAILED + str(result)


@route('/change_user_limit')
def change_user_limit():
    uid = request.query.uid
    port = request.query.port
    limit = request.query.limit

    if uid is None or port is None or limit is None:
        return FAILED + "Not enough params"

    admin = verify_login(uid)
    if admin is None:
        return FAILED + "Wrong login"

    try:
        port = int(port)
        limit= int(limit)
    except ValueError:
        return FAILED + "Wrong param type"

    result = manager.change_user_limit(admin, port, limit)
    if result is Manager.ErrType.OK:
        return OK
    else:
        return FAILED + str(result)


@route('/change_user_expire')
def change_user_expire():
    uid = request.query.uid
    port = request.query.port
    new_time = request.query.expire_time

    if uid is None or port is None or new_time is None:
        return FAILED + "Not enough params"

    admin = verify_login(uid)
    if admin is None:
        return FAILED + "Wrong login"

    try:
        port = int(port)
        new_time = float(new_time)
    except ValueError:
        return FAILED + "Wrong param type"

    result = manager.change_user_expire(admin, port, datetime.datetime.fromtimestamp(new_time))
    if result is Manager.ErrType.OK:
        return OK
    else:
        return FAILED + str(result)


@route('/change_nickname')
def change_nickname():
    uid = request.query.uid
    port = request.query.port
    nickname = request.query.nickname

    if uid is None or port is None or nickname is None:
        return FAILED + "Not enough params"

    admin = verify_login(uid)
    if admin is None:
        return FAILED + "Wrong login"

    try:
        port = int(port)
    except ValueError:
        return FAILED + "Wrong param type"

    result = manager.change_user_nickname(admin, port, nickname)
    if result is Manager.ErrType.OK:
        return OK
    else:
        return FAILED + str(result)


@route('/get_users_info')
def get_users_info():
    uid = request.query.uid

    if uid is None:
        return FAILED + "Not enough params"

    admin = verify_login(uid)
    if admin is None:
        return FAILED + "Wrong login"

    result = manager.get_users_info(admin)

    if type(result) is list:
        return str(result)
    else:
        return FAILED + str(result)


def main(argv=[]):
    logger.debug('server start')

    if len(argv) != 5:
        print("Usage: ss_manager client_addr manage_addr DB_path HTTP_port")
        return 0

    client_addr = argv[1]
    manage_addr = argv[2]
    db_addr = argv[3]
    http_port = int(argv[4])

    manager = Manager(client_addr, manage_addr, db_addr)
    server_started = manager.start_manage()
    if server_started is Manager.ErrType.OK:
        run(host='0.0.0.0', port=http_port)
        return 0
    else:
        logger.critical('Manager start failed. %s', str(server_started))
        return 1

if __name__ == '__main__':
    sys.exit(main(sys.argv))
