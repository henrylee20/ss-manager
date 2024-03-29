import socket
import json
import logging

BUF_SIZE = 1506
BACK_LOG = 10

logger = logging.getLogger('ss_manager')


class ManageConn:
    def __init__(self, client_addr, manager_addr):
        self.__sock = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)
        self.__client_addr = client_addr
        self.__manage_addr = manager_addr

    def __del__(self):
        self.close()

    def connect(self):
        if self.__sock is None:
            logger.error('Can not create socket')
            return False

        try:
            self.__sock.bind(self.__client_addr)
            self.__sock.connect(self.__manage_addr)
        except socket.error:
            logger.error('Can not bind address or connect to server')
            return False

        return True

    def get_stat(self):
        self.__sock.send(bytes("ping", encoding='ascii'))

        recv_data = self.__sock.recv(BUF_SIZE).decode('ascii')
        pos = recv_data.find("{")
        if pos is -1:
            return {}

        data = recv_data[pos:]

        status = json.loads(data)
        result = {}
        for key in status.keys():
            result[int(key)] = status[key]
        return result 

    def add_port(self, port, pwd):

        exist_ports = self.get_stat().keys()
        if port in exist_ports:
            logger.info("Port %d exist.", port)
            return False

        cmd = 'add: {"server_port": %d, "password": "%s"}' % (port, pwd)
        self.__sock.send(bytes(cmd, encoding='ascii'))
        recv = self.__sock.recv(BUF_SIZE).decode('ascii')

        if recv.find('ok') is -1:
            logger.error("Port %d add failed. server info: [%s]. Exist ports: %s", port, recv, str(exist_ports))
            return False
        else:
            return True

    def remove_port(self, port):
        exist_ports = self.get_stat().keys()
        if port not in exist_ports:
            logger.info("Port %d not exist", port)
            return False

        cmd = 'remove: {"server_port": %d}' % (port)
        self.__sock.send(bytes(cmd, encoding='ascii'))
        recv = self.__sock.recv(BUF_SIZE).decode('ascii')

        if recv.find('ok') is -1:
            logger.error("Port %d rm failed. server info: [%s]. Exist ports: %s", port, recv, str(exist_ports))
            return False
        else:
            return True

    def close(self):
        self.__sock.close()
        self.__sock = None
        self.__client_addr = ""
        self.__manage_addr = ""
