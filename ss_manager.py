import sys
import user_manage
import datetime


def main(argv=None):
    manager = user_manage.Manager('/tmp/client.sock', '/tmp/manage.sock', '/tmp/test.sqlite')

    manager.add_admin('henrylee', 'likaijie')
    admin = manager.admin_login('henrylee', 'likaijie')
    if admin is None:
        print('login err')
        return 1

    user = admin.add_user('123123123', datetime.datetime(2018, 1, 28, 0, 0), 1024*1024*1024)
    print(user)

    print("enable user: " + str(admin.enable_user(user.port)))
    print("start user" + str(admin.start_user(user.port)))
    print("start user" + str(admin.start_user(user.port)))
    #print("stop user" + str(admin.stop_user(user.port)))


if __name__ == '__main__':
    sys.exit(main(sys.argv))
