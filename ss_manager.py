import sys
import user_manage
import datetime


def main(argv=None):
    manager = user_manage.Manager('/tmp/client.sock', '/tmp/manage.sock', '/tmp/test.sqlite')

    manager.add_admin('henrylee', 'likaijie')

    print("Login: " + str(manager.admin_login('henrylee', 'likaijie')))

    port = manager.add_user('henrylee', '123123', datetime.datetime(2018, 2, 1))
    if port is 0:
        print("add port failed")
        return 1

    print("Added port: " + str(port))
    print(manager.start_user('henrylee', port))
    print(manager.enable_user('henrylee', port))


if __name__ == '__main__':
    sys.exit(main(sys.argv))
