import socket
import json

BUF_SIZE = 1506
BACK_LOG = 10


class ManageConn:
    def __init__(self, client_addr, manager_addr):
        self.__sock = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)
        self.__client_addr = client_addr
        self.__manage_addr = manager_addr

    def connect(self):
        if self.__sock is None:
            return False

        try:
            self.__sock.bind(self.__client_addr)
            self.__sock.connect(self.__manage_addr)
        except socket.error:
            return False

        return True

    def get_stat(self):
        self.__sock.send("ping")

        recv_data = self.__sock.recv(BUF_SIZE)
        pos = recv_data.find("{")
        if pos is -1:
            return {}

        data = recv_data[pos:]
        return json.loads(data)

    def add_port(self, port, pwd):
        cmd = 'add: {"server_port": %d, "password": "%s"}' % (port, pwd)
        self.__sock.send(cmd)

        recv = self.__sock.recv(BUF_SIZE)
        if recv.find('OK') is -1:
            return False
        else:
            return True

    def remove_port(self, port):
        cmd = 'remove: {"server_port": %d}' % (port)
        self.__sock.send(cmd)

        recv = self.__sock.recv(BUF_SIZE)
        if recv.find('OK') is -1:
            return False
        else:
            return True

    def close(self):
        self.__sock.close()
        self.__sock = None
        self.__client_addr = ""
        self.__manage_addr = ""
