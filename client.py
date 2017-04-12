import time, socket
from datetime import datetime, timedelta
from helper import *
from config import Message

# use for sending messages by script.
# use for testing purpose
class Client:
    time = None
    e = None
    my_ip = None
    my_port = None
    ports_info = None
    client_id = None
    client_request_id = None
    view = None
    timeout = None
    client_listen_socket = None

    def __init__(self, my_ip, my_port, client_id, ports_info, e):
        self.ports_info = ports_info
        self.client_id = client_id
        self.my_port = my_port
        self.my_ip = my_ip
        self.client_request_id = 0
        self.view = 0
        self.timeout = 2
        self.client_listen_socket = create_listen_sockets(self.my_ip, self.my_port)
        self.e = e
        print 'Client %d starts running at %s' % (self.client_id , time.ctime(int(time.time())))
        while True:
            e.wait()
            self.client_send_message()
            e.clear()

    def client_send_message(self):
        m = 'This is message %d from client %d !!!' % (self.client_request_id, self.client_id)
        msg = Message(5, None, self.client_id, self.client_request_id, None, m, None);
        encoded_msg = encode_message(msg)
        self.broadcast_msg(encoded_msg)
        print 'Client %d send message %d with timeout %d' % (self.client_id , self.client_request_id , self.timeout)

        nextTimeout = self.timeout
        while True:
            self.client_listen_socket.settimeout(nextTimeout)
            try:
                t = time.time()
                all_data = self.client_listen_socket.recv(65535)
                m = decode_message(all_data)
                if m.client_request_id == self.client_request_id:
                    print 'Client %d request %d is executed.' % (self.client_id , self.client_request_id)
                    self.client_request_id += 1
                    return
                nextTimeout = nextTimeout - (time.time() - t)
            except socket.timeout:
                # timeout
                print 'Client %d request %d timeout.' % (self.client_id , self.client_request_id)
                self.view = self.view + 1
                msg = Message(4, None, None, None, self.view, None, None)
                self.broadcast_msg(encode_message(msg))
                self.timeout *= 2
                time.sleep(0.5)
                self.client_send_message()
                return

    def broadcast_msg(self, m):
        for key in self.ports_info.keys():
            v = self.ports_info[key]
            send_message(v[0], v[1], m)
