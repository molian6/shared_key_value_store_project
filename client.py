import time, socket
from datetime import datetime, timedelta
from helper import *
from config import Message
import multiprocessing
# use for sending messages by script.
# use for testing purpose
class Client:
    my_ip = None
    my_port = None
    client_id = None
    client_request_id = None
    master_port_info = None #[ip, port]
    client_listen_socket = None
    response_listen_thread = None

    def __init__(self, my_ip, my_port, client_id, master_port_info):
        self.master_port_info = master_port_info
        self.client_id = client_id
        self.my_port = my_port
        self.my_ip = my_ip
        self.client_request_id = -1
        self.client_listen_socket = create_listen_sockets(self.my_ip, self.my_port)
        print 'Client %d starts running at %s' % (self.client_id , time.ctime(int(time.time())))
        self.response_listen_thread = multiprocessing.Process(target = self.response_listen , args = ())
        self.response_listen_thread.start()

    def send_request(self , command):
        print command
        command = command.split(' ')
        command_type = command[0]
        if len(command) > 1:
            key = command[1]
        if len(command) > 2:
            value = command[2]
        if command_type == 'put':
            command = 7
        if command_type == 'get':
            command = 8
        if command_type == 'delete':
            command = 9
        if command_type == 'addshard':
            command = 10
        self.client_send_message(command, key, value)

    def client_send_message(self, command, key, value):
        # m = 'This is message %d from client %d !!!' % (self.client_request_id, self.client_id)
        self.client_request_id += 1
        msg = Message(mtype = 5, client_id = self.client_id, client_request_id = self.client_request_id, command = command, key = key, value = value);
        send_message(self.master_port_info[0], self.master_port_info[1], encode_message(msg))
        print 'Client %d send message %d' % (self.client_id , self.client_request_id)
        # self.response_listen()

    def response_listen(self):
        while True:
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
