import time, socket
from config import Message
from datetime import datetime, timedelta
from helper import *
import math

class Master(object):
	timeout_sheet = {} # absolute time
	num_shard = None
	shard_view = []
	client_port_info = None
	shard_port_info = None
	master_port_info = None
	#sent_request = {} # req_id , received
	req_id = 0
	request_queue = [] # shard_id -> [req_id]
	shard_pos = {}
	val2_64 = math.pow(2,64)
	shard_next_timeout = []
	start_timeout = 4

	def __init__(self , shard_port_info , client_port_info , master_port_info):
		self.shard_port_info = shard_port_info
		self.client_port_info = client_port_info
		self.master_port_info = master_port_info
		self.num_shard = len(self.shard_port_info)
		for i in range(self.num_shard):
			self.shard_view.append(0)
			self.request_queue.append([])
			self.shard_pos[i] = consistent_hashing(i)
			self.shard_next_timeout.append(self.start_timeout)

		self.receive_socket = create_listen_sockets(self.master_ports_info[0], self.master_ports_info[1])
		while True:
			timeout_shard , nextTimeout = self.getTimeout()
			self.receive_socket.settimeout(nextTimeout)
			try:
        		all_data = self.receive_socket.recv(65535)
         		msg = all_data
           		self.handle_message(decode_message(msg))
           	except:
           		self.handle_timeout(timeout_shard)

    def handle_message(self , m):
    	if m.type == 5:
    		self.handle_request(self , m)
    	if m.type == 6:
    		self.handle_response(self , m)

    def handle_request(self  , m):
    	if m.command == 10:
    		# addshard
    		
    		
    	else:
    		# save get delete
    		pos = consistent_hashing(m.key)
    		shard_id = self.find_shard(pos)
    		self.request_queue[shard_id].append([m , self.req_id])
    		self.req_id += 1
    		if len(self.request_queue[shard_id]) == 1:
    			v = self.shard_port_info[shard_id][self.shard_view[shard_id]]
    			send_message(v[0] , v[1] , encode_message(msg))
    			self.timeout_sheet[shard_id] = time.time() + self.shard_next_timeout[shard_id]

    def handle_response(self , m):



    def handle_timeout(self , shard_id):



    def getTimeout(self):
    	if len(self.timeout_sheet) == 0:
    		nextTimeout = 10000
    		timeout_shard = None
    	else:
			timeout_shard = min(self.timeout_sheet , key = self.timeout_sheet.get)
			nextTimeout = self.timeout_sheet[timeout_shard] - time.time()
		return timeout_shard , nextTimeout

	def find_shard(self , val):
		best = -1
		best_dis = self.val2_64
		for i in range(self.num_shard):
			if self.shard_pos[i] < val:
				dis = val - self.shard_pos[i]
			else:
				dis = self.val2_64 + val - self.shard_pos[i]
			if dis < best_dis:
				best = i
				best_dis = dis
		return best

	def broadcast(self , shard_id , m):
		for key in self.shard_port_info[shard_id].keys():
			v = self.shard_port_info[shard_id][key]
			send_message(v[0], v[1], m)

