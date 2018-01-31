import sys
import user_manage
import datetime


def main(argv=None):
    manager = user_manage.Manager('/tmp/client.sock', '/tmp/manage.sock', '/tmp/test.sqlite')
    print('Server start: ' + str(manager.start_manage()))

    manager.add_admin('henrylee', 'likaijie')

    print("Login: " + str(manager.admin_login('henrylee', 'likaijie')))

    port = manager.add_user('henrylee', '123123', datetime.datetime(2018, 2, 1))
    if port is 0:
        print("Add port failed")
        return 1

    print("Added port: " + str(port))
    print("Start port: " + str(manager.start_user('henrylee', port)))
    print("Enable port: " + str(manager.enable_user('henrylee', port)))


if __name__ == '__main__':
    sys.exit(main(sys.argv))
