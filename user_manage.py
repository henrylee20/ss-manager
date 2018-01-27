import threading
import sqlite3
import datetime
import conn

from enum import Enum


# TODO SQL operate need to be changed for safe reason
class DBOperator:
    def __init__(self, filename):
        self.__db = sqlite3.connect(filename)
        self.__cursor = self.__db.cursor()

    def init_db(self):
        self.__cursor.execute('create table User '
                              '(port INT PRIMARY KEY, pwd VARCHAR(24), expire_time FLOAT, '
                              'trans_limit BIGINT, trans_used BIGINT, enabled INT, admin VARCHAR(24))')
        self.__cursor.execute('create table Admin '
                              '(username VARCHAR(24) PRIMARY KEY, pwd VARCHAR(24))')

    def add_user(self, port, pwd, expire_time, trans_limit, trans_used, admin_name):
        self.__cursor.execute('insert into User (port, pwd, expire_time, trans_limit, trans_used, enabled, admin) '
                              'VALUES (%d, "%s", %f, %d, %d, %d, "%s")' %
                              (port, pwd, expire_time.timestamp(), trans_limit, trans_used, 0, admin_name))

    def del_user(self, port):
        self.__cursor.execute('delete from User where port = %d' % (port))

    def enable_user(self, port):
        self.__cursor.execute('update User set enabled = 1 where port = %d' % (port))

    def disable_user(self, port):
        self.__cursor.execute('update User set enabled = 0 where port = %d' % (port))

    def change_pwd(self, port, pwd):
        self.__cursor.execute('update User set pwd = "%s" where port = %d' % (pwd, port))

    def update_used(self, port, used):
        self.__cursor.execute('update User set trans_used = %d where port = %d' % (used, port))

    def change_limit(self, port, new_limit):
        self.__cursor.execute('update User set trans_limit = %d where port = %d' % (new_limit, port))

    def change_expire(self, port, new_time):
        self.__cursor.execute('update User set expire_time = %f where port = %d' % (new_time.timestamp(), port))

    def change_admin(self, port, admin):
        self.__cursor.execute('update User set admin = "%s", where port = %d' % (admin, port))

    def get_all_users(self, admin=""):
        if len(admin):
            self.__cursor.execute('select port from User where admin = "%s"' % admin)
        else:
            self.__cursor.execute('select port from User')

        result = self.__cursor.fetchall()
        if len(result):
            return [row[0] for row in result]
        else:
            return []

    def get_user_data(self, port):
        self.__cursor.execute('select pwd, expire_time, trans_limit, trans_used, enabled, admin '
                              'from User where port = %d' % port)
        result = self.__cursor.fetchall()
        if len(result):
            return result[0][0], datetime.datetime.fromtimestamp(result[0][1]), \
                   result[0][2], result[0][3], result[0][4] is 1, result[0][5]
        else:
            return None

    def add_admin(self, name, pwd):
        self.__cursor.execute('insert into Admin (username, pwd) VALUES ("%s", "%s")' % (name, pwd))

    def del_admin(self, name):
        self.__cursor.execute('delete from Admin where username = "%s"' % (name))

    def change_admin_pwd(self, name, pwd):
        self.__cursor.execute('update Admin set pwd = "%s" where username = "%s"' % (pwd, name))

    def get_all_admins(self):
        self.__cursor.execute('select username, pwd from Admin')
        return self.__cursor.fetchall()

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
    def __init__(self, client_addr, manage_addr, db_filename):
        self.__conn = conn.ManageConn(client_addr, manage_addr)
        self.__db = DBOperator(db_filename)
        self.__admins = {}
        self.__lock_port = threading.Lock()
        self.__port_trans = {}

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

        for port in stat.keys():
            self.__port_trans[int(port)] = stat[port]

    def __verify_admin(self, admin, user):
        if admin.name is not user.admin:
            return False

        ports = self.__db.get_all_users(admin.name)
        if user.port in ports:
            return False

        return True

    def add_admin(self, username, pwd):
        self.__db.add_admin(username, pwd)
        self.__admins[username] = pwd

    def admin_login(self, username, pwd):
        if username in self.__admins.keys() and pwd is self.__admins[username]:
            ports = self.__db.get_all_users(username)

            users = {}
            for port in ports:
                info = self.__db.get_user_data(port)
                user = User(port, info[0], info[1], info[2], info[3], info[4], False, username)
                users[port] = user

            return Admin(username, users, self)
        else:
            return None

    def add_user(self, admin, user):
        if not self.__verify_admin(admin, user):
            return False

        self.__lock_port.acquire()

        port = self.__alloc_port()
        self.__db.add_user(port, user.pwd, user.expire_time, user.trans_limit, user.trans_used, admin.name)

        self.__lock_port.release()

        if port is 0xFFFF:
            return False
        else:
            return port

    def del_user(self, admin, user):
        if not self.__verify_admin(admin, user):
            return False

        self.stop_user(admin, user)
        self.__db.del_user(user.port)
        return True

    def start_user(self, admin, user):
        if not self.__verify_admin(admin, user):
            return False

        self.__conn.add_port(user.port, user.pwd)
        return True

    def stop_user(self, admin, user):
        if not self.__verify_admin(admin, user):
            return False

        self.__conn.remove_port(user.port)
        return True

    def enable_user(self, admin, user):
        if not self.__verify_admin(admin, user):
            return False

        self.__db.enable_user(user.port)
        return True

    def disable_user(self, admin, user):
        if not self.__verify_admin(admin, user):
            return False

        self.__db.disable_user(user.port)
        return True

    def change_user_pwd(self, admin, user):
        if not self.__verify_admin(admin, user):
            return False

        self.__db.change_pwd(user.port, user.pwd)
        return True

    def update_user_used(self, admin, user):
        if not self.__verify_admin(admin, user):
            return False

        self.__db.update_used(user.port, user.trans_used)
        return True

    def change_user_expire(self, admin, user):
        if not self.__verify_admin(admin, user):
            return False

        self.__db.change_expire(user.port, user.expire_time)
        return True

    def change_user_limit(self, admin, user):
        if not self.__verify_admin(admin, user):
            return False

        self.__db.change_limit(user.port, user.trans_limit)
        return True

    def change_user_admin(self, admin, user, new_admin_name):
        if not self.__verify_admin(admin, user):
            return False

        # TODO unfinished
        return False
        self.__db.change_admin(user.port, new_admin_name)
        return True

    def get_stat(self, port):
        if port in self.__port_trans.keys():
            return self.__port_trans[port]
        else:
            return -1


class User:
    def __init__(self, port=0, pwd="", expire_time=datetime.datetime(1980, 1, 1, 0, 0),
                 trans_limit=0, trans_used=0, enabled=False, started=False, admin=""):
        self.port = port
        self.pwd = pwd
        self.expire_time = expire_time
        self.trans_limit = trans_limit
        self.trans_used = trans_used
        self.enabled = enabled
        self.started = started
        self.admin = admin

    def __repr__(self):
        return repr((self.port, self.pwd, self.expire_time.strftime("%Y-%m-%d %H:%M:%S"),
                     self.trans_limit, self.trans_used, self.enabled, self.started, self.admin))


class Admin:

    class ErrType(Enum):
        OK = 0

        no_available_port = 1
        no_such_user = 2
        manager_refused = 3
        start_failed = 4
        expired = 5

    def __init__(self, name='', users={}, manager=None):
        self.name = name
        self.__users = users
        self.__manager = manager

    def __update(self):
        now = datetime.datetime.now()
        for user in self.__users.values():
            dt = (user.expire_time - now).total_seconds()
            if dt <= 0:
                self.stop_user(user.port)
                self.disable_user(user.port)
            # TODO trans_limit

    def __verify_port(self, port):
        if port not in self.__users.keys():
            return False
        return True

    def add_user(self, pwd, expire_time, trans_limit):
        new_user = User(pwd=pwd, expire_time=expire_time, trans_limit=trans_limit, admin=self.name)
        port = self.__manager.add_user(self, new_user)

        if port is False:
            return Admin.ErrType.no_available_port

        new_user.port = port
        self.__users[port] = new_user

        return new_user

    def del_user(self, port):
        if not self.__verify_port(port):
            return Admin.ErrType.no_such_user

        if self.__manager.del_user(self, self.__users[port]):
            self.__users.pop(port)
            return Admin.ErrType.OK
        else:
            return Admin.ErrType.manager_refused

    def start_user(self, port):
        if not self.__verify_port(port):
            return Admin.ErrType.no_such_user

        # TODO Check limit
        now = datetime.datetime.now()
        if (self.__users[port].expire_time - now).total_seconds() <= 0:
            return Admin.ErrType.expired

        if self.__manager.start_user(self, self.__users[port]):
            self.__users[port].started = True
            return Admin.ErrType.OK
        else:
            return Admin.ErrType.start_failed

    def stop_user(self, port):
        if not self.__verify_port(port):
            return Admin.ErrType.no_such_user

        if self.__manager.stop_user(self, self.__users[port]):
            self.__users[port].started = False
            return Admin.ErrType.OK
        else:
            return Admin.ErrType.manager_refused

    def enable_user(self, port):
        if not self.__verify_port(port):
            return Admin.ErrType.no_such_user

        if self.__manager.enable_user(self, self.__users[port]):
            self.__users[port].enabled = True
            return Admin.ErrType.OK
        else:
            return Admin.ErrType.manager_refused

    def disable_user(self, port):
        if not self.__verify_port(port):
            return Admin.ErrType.no_such_user

        if self.__manager.disable_user(self, self.__users[port]):
            self.__users[port].enabled = False
            return Admin.ErrType.OK
        else:
            return Admin.ErrType.manager_refused

    def change_user_pwd(self, port, pwd):
        if not self.__verify_port(port):
            return Admin.ErrType.no_such_user

        old_pwd = self.__users[port].pwd
        self.__users[port].pwd = pwd

        if self.__manager.change_user_pwd(self, self.__users[port]):
            result = self.stop_user(port)
            if result is not Admin.ErrType.OK:
                return result
            result = self.start_user(port)
            return result
        else:
            self.__users[port].pwd = old_pwd
            return Admin.ErrType.manager_refused

    def change_user_expire(self, port, expire_time):
        if not self.__verify_port(port):
            return Admin.ErrType.no_such_user

        old_time = self.__users[port].expire_time
        self.__users[port].expire_time = expire_time

        if self.__manager.change_user_expire(self, self.__users[port]):
            return Admin.ErrType.OK
        else:
            self.__users[port].expire_time = old_time
            return Admin.ErrType.manager_refused

    def change_user_limit(self, port, trans_limit):
        if not self.__verify_port(port):
            return Admin.ErrType.no_such_user

        old_limit = self.__users[port].trans_limit
        self.__users[port].trans_limit = trans_limit

        if self.__manager.change_user_limit(self, self.__users[port]):
            return Admin.ErrType.OK
        else:
            self.__users[port].trans_limit = old_limit
            return Admin.ErrType.manager_refused
