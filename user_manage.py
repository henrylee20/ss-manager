import threading
import sqlite3
import datetime
import time
import logging
import os
import conn

from enum import Enum

logger = logging.getLogger('ss_manager')


# TODO SQL operate need to be changed for safe reason
class DBOperator:
    def __init__(self, filename):
        self.__db = sqlite3.connect(filename, check_same_thread=False)

    def __del__(self):
        self.__db.close()

    def init_db(self):
        logger.info('Init DB')
        cursor = self.__db.cursor()
        cursor.execute('create table User '
                       '(port INT PRIMARY KEY, pwd VARCHAR(24), expire_time FLOAT, '
                       'trans_limit BIGINT, trans_used BIGINT, enabled INT, admin VARCHAR(24))')
        cursor.execute('create table Admin '
                       '(username VARCHAR(24) PRIMARY KEY, pwd VARCHAR(24))')
        cursor.close()
        self.__db.commit()

    def add_user(self, port, pwd, expire_time, trans_limit, trans_used, admin_name):
        cursor = self.__db.cursor()
        cursor.execute('insert into User (port, pwd, expire_time, trans_limit, trans_used, enabled, admin) '
                       'VALUES (%d, "%s", %f, %d, %d, %d, "%s")' %
                       (port, pwd, expire_time.timestamp(), trans_limit, trans_used, 0, admin_name))
        cursor.close()
        self.__db.commit()

    def del_user(self, port):
        cursor = self.__db.cursor()
        cursor.execute('delete from User where port = %d' % (port))
        cursor.close()
        self.__db.commit()

    def enable_user(self, port):
        cursor = self.__db.cursor()
        cursor.execute('update User set enabled = 1 where port = %d' % (port))
        cursor.close()
        self.__db.commit()

    def disable_user(self, port):
        cursor = self.__db.cursor()
        cursor.execute('update User set enabled = 0 where port = %d' % (port))
        cursor.close()
        self.__db.commit()

    def change_pwd(self, port, pwd):
        cursor = self.__db.cursor()
        cursor.execute('update User set pwd = "%s" where port = %d' % (pwd, port))
        cursor.close()
        self.__db.commit()

    def update_used(self, port, used):
        cursor = self.__db.cursor()
        cursor.execute('update User set trans_used = %d where port = %d' % (used, port))
        cursor.close()
        self.__db.commit()

    def change_limit(self, port, new_limit):
        cursor = self.__db.cursor()
        cursor.execute('update User set trans_limit = %d where port = %d' % (new_limit, port))
        cursor.close()
        self.__db.commit()

    def change_expire(self, port, new_time):
        cursor = self.__db.cursor()
        cursor.execute('update User set expire_time = %f where port = %d' % (new_time.timestamp(), port))
        cursor.close()
        self.__db.commit()

    def change_admin(self, port, admin):
        cursor = self.__db.cursor()
        cursor.execute('update User set admin = "%s", where port = %d' % (admin, port))
        cursor.close()
        self.__db.commit()

    def get_all_users(self, admin=""):
        cursor = self.__db.cursor()
        if len(admin):
            cursor.execute('select port from User where admin = "%s"' % admin)
        else:
            cursor.execute('select port from User')

        result = cursor.fetchall()
        logger.debug('result: %s', str(result))
        cursor.close()
        if len(result):
            return [row[0] for row in result]
        else:
            logger.warning('Got no result')
            return []

    def get_user_data(self, port):
        cursor = self.__db.cursor()
        cursor.execute('select pwd, expire_time, trans_limit, trans_used, enabled, admin '
                       'from User where port = %d' % port)
        result = cursor.fetchall()
        cursor.close()
        if len(result):
            return result[0][0], datetime.datetime.fromtimestamp(result[0][1]), \
                   result[0][2], result[0][3], result[0][4] is 1, result[0][5]
        else:
            logger.warning('Got no data')
            return None, None, None, None, None, None

    def get_enabled_users(self):
        cursor = self.__db.cursor()
        cursor.execute('select port, admin from User where enabled = 1')
        result = cursor.fetchall()
        cursor.close()

        return result

    def add_admin(self, name, pwd):
        cursor = self.__db.cursor()

        try:
            cursor.execute('insert into Admin (username, pwd) VALUES ("%s", "%s")' % (name, pwd))
        except sqlite3.IntegrityError:
            logger.warning('Admin name exist')
            return False

        cursor.close()
        self.__db.commit()
        return True

    def del_admin(self, name):
        cursor = self.__db.cursor()
        cursor.execute('delete from Admin where username = "%s"' % (name))
        cursor.close()
        self.__db.commit()

    def change_admin_pwd(self, name, pwd):
        cursor = self.__db.cursor()
        cursor.execute('update Admin set pwd = "%s" where username = "%s"' % (pwd, name))
        cursor.close()
        self.__db.commit()

    def get_all_admins(self):
        cursor = self.__db.cursor()
        cursor.execute('select username, pwd from Admin')
        result = cursor.fetchall()
        cursor.close()
        return result


class Manager:
    class ErrType(Enum):
        OK = 0
        permission_denied = 1
        user_expired = 2
        user_reached_limit = 3
        wrong_username_or_pwd = 4
        user_exist = 5
        alloc_port_failed = 6
        server_refused = 7
        server_not_connect = 8
        port_closed = 9

    def __init__(self, client_addr, manage_addr, db_filename):
        if os.path.exists(client_addr):
            logger.warning("Client socket exist! deleting it. File: %s", client_addr)
            os.remove(client_addr)

        self.__conn = conn.ManageConn(client_addr, manage_addr)
        self.__db = DBOperator(db_filename)
        self.__admins = {}
        self.__lock_port = threading.Lock()
        self.__port_trans = {}

        self.__get_all_admin()

        self.__manage_thread = threading.Thread(target=self.manage_thread, args=(self,))
        self.__manage_thread_is_run = False

    def start_manage(self):
        if not self.__conn.connect():
            return Manager.ErrType.server_not_connect
        logger.debug('SS Server connected')

        enabled_users = self.__db.get_enabled_users()

        for user in enabled_users:
            result = self.start_user(user[1], user[0])
            logger.info('Starting enabled user %d, result: %s', user[0], str(result))

        self.__manage_thread_is_run = True
        self.__manage_thread.start()

        return Manager.ErrType.OK

    def stop_manage(self):
        self.__manage_thread_is_run = False

    def manage_thread(self, arg):
        logger.debug('Manager thread started')
        while self.__manage_thread_is_run:
            self.__update_stat()
            time.sleep(60)

    def __get_all_admin(self):
        try:
            admins = self.__db.get_all_admins()
            for admin in admins:
                self.__admins[admin[0]] = admin[1]
        except sqlite3.OperationalError:
            logger.warning('Can not find table in DB, start init DB')
            self.__db.init_db()

    @staticmethod
    def __find_available_port(exist_ports):
        result = 0xFFFF
        min_port = 23331

        if len(exist_ports):
            min_port = min(exist_ports)

        for possible in range(min_port, 0xFFFF):
            if possible not in exist_ports:
                result = possible
                break

        return result

    def __alloc_port(self):
        ports = []
        for admin in self.__admins.keys():
            ports.extend(self.__db.get_all_users(admin))

        port = self.__find_available_port(ports)
        return port

    def __update_stat(self):
        stat = self.__conn.get_stat()
        self.__port_trans.clear()

        now = datetime.datetime.now()

        for port in stat.keys():
            port = int(port)
            self.__port_trans[port] = stat[port]
            _, expire_time, trans_limit, trans_used, _, admin = self.__db.get_user_data(port)

            # TODO: err handle: get NULL DATA
            if expire_time is None:
                logger.warning('port %d got empty info', port)
                continue

            dt = expire_time - now
            if dt.total_seconds() <= 0 or (trans_limit != -1 and trans_used + stat[port] > trans_limit):
                logger.info('port %d expired or reach the limit.Expire time: %s, Limit: %d, Used: %d',
                            expire_time.strftime('%Y-%m-%d %H:%M:%S'), trans_limit, trans_used)
                self.stop_user(admin, port)
                self.disable_user(admin, port)

    def __verify_admin(self, admin, user):
        users = self.__db.get_all_users(admin)
        if user not in users:
            logger.info('verify_admin failed, admin: %s, user: %d', admin, user)
            return False

        return True

    def add_admin(self, username, pwd):
        if username == "" or pwd == "":
            return Manager.ErrType.wrong_username_or_pwd

        if self.__db.add_admin(username, pwd):
            self.__admins[username] = pwd
            return Manager.ErrType.OK
        else:
            return Manager.ErrType.user_exist

    def admin_login(self, username, pwd):
        if username in self.__admins.keys() and pwd == self.__admins[username]:
            return Manager.ErrType.OK
        else:
            logger.debug("admins:" + str(self.__admins) + (', input: [%s], [%s]' % (username, pwd)))
            logger.debug("test result:" + str(username in self.__admins.keys()) + " and " + str(pwd is self.__admins[username]))
            return Manager.ErrType.wrong_username_or_pwd

    def add_user(self, admin, pwd, expire_time, trans_limit=-1, trans_used=0):
        self.__lock_port.acquire()

        port = self.__alloc_port()
        self.__db.add_user(port, pwd, expire_time, trans_limit, trans_used, admin)

        self.__lock_port.release()

        if port is 0xFFFF:
            return Manager.ErrType.alloc_port_failed
        else:
            return port

    def del_user(self, admin, user):
        if not self.__verify_admin(admin, user):
            return Manager.ErrType.permission_denied

        self.stop_user(admin, user)
        self.__db.del_user(user)
        return Manager.ErrType.OK

    def start_user(self, admin, user):
        if not self.__verify_admin(admin, user):
            return Manager.ErrType.permission_denied

        pwd, expire_time, trans_limit, trans_used, _, admin = self.__db.get_user_data(user)
        dt = expire_time - datetime.datetime.now()

        if dt.total_seconds() <= 0:
            return Manager.ErrType.user_expired

        if trans_limit != -1 and trans_limit < trans_used + self.__port_trans[user]:
            return Manager.ErrType.user_reached_limit

        if self.__conn.add_port(user, pwd):
            return Manager.ErrType.OK
        else:
            return Manager.ErrType.server_refused

    def stop_user(self, admin, user):
        if not self.__verify_admin(admin, user):
            return Manager.ErrType.permission_denied

        if self.__conn.remove_port(user):
            return Manager.ErrType.OK
        else:
            return Manager.ErrType.port_closed

    def enable_user(self, admin, user):
        if not self.__verify_admin(admin, user):
            return Manager.ErrType.permission_denied

        self.__db.enable_user(user)
        return Manager.ErrType.OK

    def disable_user(self, admin, user):
        if not self.__verify_admin(admin, user):
            return Manager.ErrType.permission_denied

        self.__db.disable_user(user)
        return Manager.ErrType.OK

    def change_user_pwd(self, admin, user, pwd):
        if not self.__verify_admin(admin, user):
            return Manager.ErrType.permission_denied

        self.__db.change_pwd(user, pwd)
        return Manager.ErrType.OK

    def update_user_used(self, admin, user, trans_used):
        if not self.__verify_admin(admin, user):
            return Manager.ErrType.permission_denied

        self.__db.update_used(user, trans_used)
        return Manager.ErrType.OK

    def change_user_expire(self, admin, user, expire_time):
        if not self.__verify_admin(admin, user):
            return Manager.ErrType.permission_denied

        self.__db.change_expire(user, expire_time)
        return Manager.ErrType.OK

    def change_user_limit(self, admin, user, trans_limit):
        if not self.__verify_admin(admin, user):
            return Manager.ErrType.permission_denied

        self.__db.change_limit(user, trans_limit)
        return Manager.ErrType.OK

    def change_user_admin(self, admin, user, new_admin_name):
        if not self.__verify_admin(admin, user):
            return Manager.ErrType.permission_denied

        self.__db.change_admin(user, new_admin_name)
        return Manager.ErrType.OK

    def get_users_info(self, admin):
        if admin not in self.__admins.keys():
            return Manager.ErrType.permission_denied

        result = []
        ports = self.__db.get_all_users(admin)
        for port in ports:
            pwd, expire_time, trans_limit, trans_used, enabled, _ = self.__db.get_user_data(port)
            started = port in self.__port_trans.keys()
            if started:
                trans_used += self.__port_trans[port]
                started = 1
            else:
                started = 0

            if enabled:
                enabled = 1
            else:
                enabled = 0

            user = {'port': port, 'pwd': pwd, 'expire_time': expire_time.timestamp(), 'trans_limit': trans_limit,
                    'trans_used': trans_used, 'enabled': enabled, 'started': started, 'admin': admin}
            result.append(user)

        return result

    def get_stat(self, port):
        if port in self.__port_trans.keys():
            return self.__port_trans[port]
        else:
            return Manager.ErrType.wrong_username_or_pwd
