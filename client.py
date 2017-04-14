import time, socket
from datetime import datetime, timedelta
from helper import *
from config import Message
# from threading import Thread
# use for sending messages by script.
# use for testing purpose
class Client:
    e = None
    my_ip = None
    my_port = None
    client_id = None
    client_request_id = None
    master_port_info = None #[ip, port]
    client_listen_socket = None

    def __init__(self, my_ip, my_port, client_id, master_port_info, e):
        self.master_port_info = master_port_info
        self.client_id = client_id
        self.my_port = my_port
        self.my_ip = my_ip
        self.client_request_id = -1
        self.e = e
        self.client_listen_socket = create_listen_sockets(self.my_ip, self.my_port)

        print 'Client %d starts running at %s' % (self.client_id , time.ctime(int(time.time())))

        response_listen_thread = Thread(target=self.response_listen)
        response_listen_thread.start()

        while True:
            e.wait()
            key = None
            value = None
            command = None
            command_type = int(raw_input("1: put\n 2: get\n 3: delete\n 4:addShard"))
            if command_type == 1:
                key = raw_input("PUT Enter key: ")
                value = raw_input("PUT Enter value: ")
                command = 7
            if command_type == 2:
                key = raw_input("GET Enter key: ")
                command = 8
            if command_type == 3:
                key = raw_input("DELETE Enter key: ")
                command = 9
            if command_type ==4:
                command = 10

            self.client_send_message(command, key, value)
            e.clear()

    def client_send_message(self, command, key, value):
        # m = 'This is message %d from client %d !!!' % (self.client_request_id, self.client_id)
        self.client_request_id += 1
        msg = Message(mtype = 5, client_id = self.client_id, client_request_id = self.client_request_id, command = command, key = key, value = value);
        send_message(self.master_port_info[0], self.master_port_info[1], encode_message(msg))
        print 'Client %d send message %d' % (self.client_id , self.client_request_id)
        # self.response_listen()

    def response_listen():
        while True:
            try:
                all_data = self.client_listen_socket.recv(65535)
                m = decode_message(all_data)
                if m.command == 7:
                    print 'put(%s, %s) successfully!' % (m.key, m.value)
                if m.command == 8:
                    print 'get(%s) successfully, value is %s!' % (m.key, m.value)
                if m.command == 9:
                    print 'delete(%s) successfully!' % (m.key)
                if m.command == 7:
                    print 'addShard successfully!'
                return
            except socket.timeout:
                print 'client %d request %d timeout' % (self.client_id, self.client_request_id)
                return
