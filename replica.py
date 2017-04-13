import time, socket
from config import Message
from datetime import datetime, timedelta
from helper import *
# Implement the protocol each replica will run
class Replica(object):
    debug = False
    uid = None
    f = None
    view = None
    ports_info = None #a map uid-> [ip, ports]
    client_ports_info = None #a map client_id-> [ip, ports]
    request_count = {} # (req_id,command,key,value) -> count
    received_propose_list = {} #req_id -> [client_id, proposor, command , key , value, client_request_id, executed]
    learned_list = {} # req_id -> [command, key, value , executed, client_id, client_request_id]
    request_mapping = {} #(client_id, client_request_id) -> req_id
    log_file = None
    skip = False
    dic = {} #key->value

    num_followers = None
    last_exec_req = None

    def __init__(self, f, uid, ports_info, client_ports_info, debug, skip):
        self.uid = uid
        self.f = f
        self.ports_info = ports_info
        self.client_ports_info = client_ports_info
        self.last_exec_req = -1
        self.receive_socket = create_listen_sockets(self.ports_info[self.uid][0], self.ports_info[self.uid][1])
        self.view = -1
        self.debug = debug
        self.skip = skip
        print "replica %d starts running at %s." % (self.uid , time.ctime(int(time.time())))
        self.log_file = 'log%d.txt'%(self.uid)
        with open(self.log_file , 'w') as fid:
            fid.write('Log for replica %d:\n' % (self.uid))

        if (self.uid == 0):
            time.sleep(3)
            self.beProposor()

        while True:
            all_data = self.receive_socket.recv(65535)
            msg = all_data
            self.handle_message(decode_message(msg))

    def handle_message(self, m):
        if (m.mtype == 0):
            self.handle_IAmYourLeader(m)
        elif (m.mtype == 1):
            self.handle_YouAreMyLeader(m)
        elif (m.mtype == 2):
            self.handle_ProposeValue(m)
        elif (m.mtype == 3):
            self.handle_AcceptValue(m)
        elif (m.mtype == 4):
            self.handle_TimeOut(m)
        elif (m.mtype == 5):
            self.handle_Request(m)

    def broadcast_msg(self, m):
        for key in self.ports_info.keys():
            v = self.ports_info[key]
            send_message(v[0], v[1], m)

    def logging(self, req_id, command, key, value, client_id, client_request_id):
        if self.last_exec_req + 1 == req_id:
            # logging
            self.last_exec_req += 1
            # with open(self.log_file , 'a') as fid:
            #     fid.write('request %d: %s\n'%(req_id , value))
            #     print 'Replica %d writes message %d to log.' % (self.uid , req_id)
            self.learned_list[req_id] = [command, key, value , True, client_id, client_request_id]
            if command != "NOOP":
                #send logging message to client
                if command == 7:
                    if type(key) != list:
                        key = [key]
                    for i,k in enumrate(key):
                        self.dic[k] = value[i]
                elif command == 8:
                    value = self.dic[key]
                elif command == 9:
                    if key in self.dic.keys():
                        del self.dic[key]
                elif command == 10:
                    del_val = []
                    del_key = []
                    for k in self.dic.keys():
                        pos = consistent_hashing(k)
                        dis_old = distance(pos , key[0])
                        dis_new = distance(pos , key[1])
                        if dis_new < dis_old:
                            del_key.append(k)
                            del_val.append(self.dic[k])
                            del self.dic[k]
                    key = del_key
                    value = del_value

                self.received_propose_list[req_id][-1] = True
                msg = Message(mtype = 6, client_request_id = client_request_id, key = key, value = value , command = command)
                send_message(self.client_ports_info[client_id][0], self.client_ports_info[client_id][1], encode_message(msg))

            if req_id+1 in self.learned_list and self.learned_list[req_id+1][1] == False:
                self.logging(req_id+1, self.learned_list[req_id+1][0], self.learned_list[req_id+1][1], self.learned_list[req_id+1][2], self.learned_list[req_id+1][4], self.learned_list[req_id+1][5])
        else:
            self.learned_list[req_id] = [command, key, value , False, client_id, client_request_id]

    def beProposor(self):
        self.num_followers = 0
        self.request_mapping = {}
        msg = Message(mtype = 0, sender_id = self.uid)
        self.broadcast_msg(encode_message(msg))

    def handle_IAmYourLeader(self, m):
        if self.debug:
            print 'handle_IAmYourLeader', m
        # if sender_id > view, update self.view
        # send YouAreMyLeader back with message = jsonify received_propose_list
        if m.sender_id >= self.view:
            self.view = m.sender_id
            msg = Message(mtpye = 1, sender_id = self.uid, received_propose_list = self.received_propose_list)
            send_message(self.ports_info[self.view][0], self.ports_info[self.view][1], encode_message(msg))

    def handle_YouAreMyLeader(self, m):
        # update the most recent value for each blank in received_propose_list.
        self.num_followers += 1
        for key in m.received_propose_list.keys():
            x = m.received_propose_list[key]
            key = int(key)
            # if update every value to the newest proposer value
            if key not in self.received_propose_list.keys():
                self.received_propose_list[key] = x
            elif x[1] > self.received_propose_list[key][1]:
                self.received_propose_list[key] = x
            if x[1] == self.received_propose_list[key][1] and x[2] == 8 and x[-1] == True:
                self.received_propose_list[key][-1] = True

        if self.num_followers == self.f + 1:
            #   fill the holes with NOOP
            if len(self.received_propose_list) > 0:
                for i in range(0,max(self.received_propose_list.keys(), key = int)):
                    if not i in self.received_propose_list:
                        self.received_propose_list[i] = [-1, self.uid, "NOOP", None , None , None, False]
            #   propose everything in the list
            for key in self.received_propose_list.keys():
                x = self.received_propose_list[key]
                msg = Message(mtype=2, request_id=key, client_id=x[0], client_request_id=x[-2], sender_id=self.uid, command=x[2], key=x[3],value=x[4])
                self.broadcast_msg(encode_message(msg))
                if x[2] != 'NOOP':
                    self.request_mapping[(x[0] , x[-2])] = int(key)
            print "replica %d becomes leader!!! view %d" % (self.uid , self.view)
            #   propose everything in waiting_request_list
            while len(self.waiting_request_list) != 0:
                m = self.waiting_request_list.pop(0)
                if (m.client_id , m.client_request_id) not in self.request_mapping.keys():
                    # edit message
                    m.sender_id = self.uid
                    #req_id is next index in request_mapping
                    if len(self.request_mapping) == 0: req_id = 0
                    else: req_id = max(self.request_mapping.values()) + 1
                    m.request_id = req_id
                    # change message type to proposeValue
                    m.mtype = 2
                    # encode message
                    msg = encode_message(m)
                    # broadcast message
                    self.broadcast_msg(msg)
                    # add req_id to mapping list
                    self.request_mapping[(m.client_id , m.client_request_id)] = req_id

    def handle_ProposeValue(self, m):
        if self.debug: print 'handle_ProposeValue', m.client_id, m.client_request_id
        #   update received_propose_list
        #   broadcast AcceptValue(proposorid + req_id + value)
        if m.sender_id >= self.view:
            self.view = m.sender_id
            self.received_propose_list[m.request_id] = [m.client_id, m.sender_id, m.value, m.client_request_id]
            m.type = 3
            m.sender_id = self.uid
            self.broadcast_msg(encode_message(m))

    def handle_AcceptValue(self, m):
        if self.debug: print 'handle_AcceptValue', m.client_id, m.client_request_id
        # if any value reach the majority, do logging
        p = (m.request_id, m.command, m.key, m.value)
        if p not in self.request_count:
            self.request_count[p] = 1
        else:
            self.request_count[p] += 1
        if self.request_count[p] == self.f + 1:
            self.logging(m.request_id, m.command, m.key, m.value, m.client_id, m.client_request_id)

    def handle_TimeOut(self, m):
        if self.debug: print 'handle_TimeOut', m
        if self.view < m.sender_id: #ugly implementation sender_id here means client view
            self.view = m.sender_id
            if (self.view == self.uid):
                self.beProposor()

    def handle_Request(self, m):
        if self.view == self.uid:
            if self.num_followers >= self.f + 1:
                # has enough followers
                if (m.client_id , m.client_request_id) not in self.request_mapping.keys():
                    # edit message
                    m.sender_id = self.uid
                    #req_id is next index in request_mapping
                    if len(self.request_mapping) == 0: req_id = 0
                    else: req_id = max(self.request_mapping.values()) + 1
                    m.request_id = req_id
                    # change message type to proposeValue
                    m.mtype = 2
                    # encode message
                    msg = encode_message(m)
                    # broadcast message
                    self.request_mapping[(m.client_id , m.client_request_id)] = req_id
                    if self.skip:
                        if req_id % 10 == 3:
                            return 
                    self.broadcast_msg(msg)
                    # add req_id to mapping list
                    
            else:
                # waitting for followers, add request to waitlist
                self.waiting_request_list.append(m)
