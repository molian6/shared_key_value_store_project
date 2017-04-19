import time, socket
from datetime import datetime, timedelta
from helper import *
from config import Message
import multiprocessing
import numpy as np
import math
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
        key = None
        value = None
        command = command.split(' ')
        command_type = command[0]
        print command_type
        if len(command) > 1:
            key = command[1]
        if len(command) > 2:
            value = command[2]
        if command_type == 'put':
            command = 7
            self.client_send_message(command, key, value)
        elif command_type == 'get':
            command = 8
            self.client_send_message(command, key, value)
        elif command_type == 'delete':
            command = 9
            self.client_send_message(command, key, value)
        elif command_type == 'addshard':
            command = 10
            self.client_send_message(command, key, value)
        elif command_type == 'batch':
            key_list = ['0']
            num = int(command[1])
            for i in range(num):
                command = int(np.random.rand()*5)+5
                if command <= 6: command = 7
                if command == 7:
                    key = int(np.random.rand()*math.pow(2,64)-1)
                    key = chr(ord('a')+key%26)+str(time.time())
                    key_list.append(key)
                    value = int(np.random.rand()*math.pow(2,64)-1)
                    value = 'x'+str(value)
                else:
                    key = int(np.random.rand()*(len(key_list)))
                    key = key_list[key]
                    value = None
                self.client_send_message(command , key , value)
                print command , key , value
        else:
            print 'illegal command'
            return

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
                print 'put(%s, %s) successfully!' % (str(m.key), str(m.value))
            if m.command == 8:
                if m.value[0] == 'error':
                    print 'get (%s) failed! (%s) does not exist in dict.' % (str(m.key) , str(m.key))
                else:
                    print 'get (%s) successfully, value is %s!' % (str(m.key), str(m.value))
            if m.command == 9:
                if m.value[0] == 'error':
                    print 'delete (%s) failed! (%s) does not exist in dict.' % (str(m.key) , str(m.key))
                else:
                    print 'delete(%s) (%s) successfully!' % (str(m.key) , str(m.value))
            if m.command == 10:
                print 'addShard successfully!'
