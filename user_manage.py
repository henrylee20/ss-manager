import threading
import sqlite3
import datetime
import time
import conn

from enum import Enum


# TODO SQL operate need to be changed for safe reason
class DBOperator:
    def __init__(self, filename):
        self.__db = sqlite3.connect(filename)

    def __del__(self):
        self.__db.close()

    def init_db(self):
        print("Init DB")
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
        print(admin + " get_all_users() result: " + str(result))
        cursor.close()
        if len(result):
            return [row[0] for row in result]
        else:
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

    @staticmethod
    def debug():
        op = DBOperator("test.sqlite")
        op.init_db()
        op.add_admin('henry', '123')
        op.add_user(2333, 'likaijie', datetime.datetime(2088, 12, 30, 12, 00), 123, 123, 'henry')
        op.add_user(23331, 'likaijie', datetime.datetime(2018, 1, 22, 12, 00), 222, 0, 'henry')
        op.add_user(23332, 'likaijie', datetime.datetime(2028, 1, 11, 12, 00), 222, 0, 'henry')
        print(op.get_all_users())
        print([op.get_user_data(user) for user in op.get_all_users()])


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
        port_closed = 8

    def __init__(self, client_addr, manage_addr, db_filename):
        self.__conn = conn.ManageConn(client_addr, manage_addr)
        self.__db = DBOperator(db_filename)
        self.__admins = {}
        self.__lock_port = threading.Lock()
        self.__port_trans = {}

        self.__get_all_admin()

        self.__conn.connect()
        self.__manage_thread = threading.Thread(target=self.manage_thread, args=(self,))
        self.__manage_thread_is_run = False

    def start_manage(self):
        enabled_users = self.__db.get_enabled_users()

        for user in enabled_users:
            result = self.start_user(user[1], user[0])
            print(str(user[0]) + ': ' + str(result))

        self.__manage_thread_is_run = True
        self.__manage_thread.start()
        self.__manage_thread.join()

    def stop_manage(self):
        self.__manage_thread_is_run = False

    def manage_thread(self):
        while self.__manage_thread_is_run:
            self.__update_stat()
            time.sleep(60)

    def __get_all_admin(self):
        try:
            admins = self.__db.get_all_admins()
            for admin in admins:
                self.__admins[admin[0]] = admin[1]
        except sqlite3.OperationalError:
            self.__db.init_db()

    @staticmethod
    def __find_available_port(exist_ports):
        result = 0xFFFF
        min_port = 23000

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
            self.__port_trans[int(port)] = stat[port]
            _, expire_time, trans_limit, trans_used, _, admin = self.__db.get_user_data(port)

            # TODO: err handle: get NULL DATA
            if expire_time is None:
                continue

            dt = expire_time - now
            if dt.total_seconds() <= 0 or (trans_limit != -1 and trans_used + stat[port] > trans_limit):
                self.stop_user(admin, port)
                self.disable_user(admin, port)

    def __verify_admin(self, admin, user):
        users = self.__db.get_all_users(admin)
        if user not in users:
            print('verify_admin failed, admin: ' + admin + ', user: ' + user)
            return False

        return True

    def add_admin(self, username, pwd):
        if self.__db.add_admin(username, pwd):
            self.__admins[username] = pwd
            return Manager.ErrType.OK
        else:
            return Manager.ErrType.user_exist

    def admin_login(self, username, pwd):
        if username in self.__admins.keys() and pwd is self.__admins[username]:
            return Manager.ErrType.OK
        else:
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
        return True

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
        return True

    def disable_user(self, admin, user):
        if not self.__verify_admin(admin, user):
            return Manager.ErrType.permission_denied

        self.__db.disable_user(user)
        return True

    def change_user_pwd(self, admin, user, pwd):
        if not self.__verify_admin(admin, user):
            return Manager.ErrType.permission_denied

        self.__db.change_pwd(user, pwd)
        return True

    def update_user_used(self, admin, user, trans_used):
        if not self.__verify_admin(admin, user):
            return Manager.ErrType.permission_denied

        self.__db.update_used(user, trans_used)
        return True

    def change_user_expire(self, admin, user, expire_time):
        if not self.__verify_admin(admin, user):
            return Manager.ErrType.permission_denied

        self.__db.change_expire(user, expire_time)
        return True

    def change_user_limit(self, admin, user, trans_limit):
        if not self.__verify_admin(admin, user):
            return Manager.ErrType.permission_denied

        self.__db.change_limit(user, trans_limit)
        return True

    def change_user_admin(self, admin, user, new_admin_name):
        if not self.__verify_admin(admin, user):
            return Manager.ErrType.permission_denied

        self.__db.change_admin(user, new_admin_name)
        return True

    def get_stat(self, port):
        if port in self.__port_trans.keys():
            return self.__port_trans[port]
        else:
            return Manager.ErrType.wrong_username_or_pwd
