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
	shard_port_info = None #[shard_id][replica_id]->[ip, port]
	master_port_info = None
	#sent_request = {} # req_id , received
	req_id = 0
	request_queue = {} # shard_id -> [req1, req2 req1 here is [m, req_id]
	req_to_shard_map = {}
	shard_pos = {}
	val2_64 = long(math.pow(2,64))
	shard_next_timeout = []
	start_timeout = 4
	#block = [] # set to True to block the new shard

	def __init__(self , shard_port_info , client_port_info , master_port_info, num_failures):
		self.shard_port_info = shard_port_info
		if shard_port_info == None:
			# if there is no shard, add a new shard
			self.handle_request(Message(mtype=5 , command = 10))
		self.client_port_info = client_port_info
		self.master_port_info = master_port_info
		self.num_failures = num_failures
		self.num_shard = len(self.shard_port_info)
		for i in range(self.num_shard):
			self.shard_view.append(0)
			self.request_queue[i] = []
			self.shard_pos[i] = consistent_hashing(str(i*10))
			self.shard_next_timeout.append(self.start_timeout)

		self.receive_socket = create_listen_sockets(self.master_port_info[0], self.master_port_info[1])
		while True:
			timeout_shard , nextTimeout = self.getTimeout()
			print timeout_shard , nextTimeout
			self.receive_socket.settimeout(nextTimeout)
			try:
				all_data = self.receive_socket.recv(65535)
				msg = all_data
		   		self.handle_message(decode_message(msg))
		   	except socket.timeout:
		   		self.handle_timeout(timeout_shard)

	def handle_message(self , m):
		if m.mtype == 5:
			self.handle_request(m)
		if m.mtype == 6:
			self.handle_response(m)

	def handle_request(self  , m):
		print 'handle request: ', m.command, m.key, m.value
		if m.command == 10:
			# addshard
			self.num_shard += 1
			self.shard_view.append(0)
			self.request_queue[self.num_shard-1] = []
			# new_shard_pos = consistent_hashing(str(self.num_shard - 1))
			# shard_id = self.find_shard(new_shard_pos, self.num_shard - 1)
			self.shard_pos[self.num_shard-1] = consistent_hashing(str((self.num_shard - 1)*10))
			self.shard_next_timeout.append(self.start_timeout)
			# read new shard's ports into self.shard_port_info
			self.shard_port_info.append(read_ports_info(m.key, self.num_failures*2+1))
			# create replicas
			# send to old shard
			# new save command to the queue of new shard, but do not send.
			if self.num_shard == 1: return
			shard_id = self.find_shard(self.shard_pos[self.num_shard-1], self.num_shard - 1)
			m.key = [self.shard_pos[shard_id] , self.shard_pos[self.num_shard-1] ]# for old shard, delete keys between m.key[0] and m.key[1]
			m.client_id = self.num_shard-1 # client_id record the id of new shard, for the use of save
			msg = Message(mtype = 5 , command = 7 , client_request_id = self.req_id)
			self.request_queue[self.num_shard-1].append([msg, self.req_id])
			self.req_to_shard_map[self.req_id] = self.num_shard-1
			self.req_id += 1

		else:
			# save get delete
			pos = consistent_hashing(m.key)
			shard_id = self.find_shard(pos, self.num_shard)

		print "shard_id is %d" %shard_id
		m.client_request_id = self.req_id
		self.request_queue[shard_id].append([m ,self.req_id])
		self.req_to_shard_map[self.req_id] = shard_id
		self.req_id += 1
   		if len(self.request_queue[shard_id]) == 1:
			print "executed immediately"
   			self.send_request(shard_id , m)

	def handle_response(self , m):
		print 'handle response', m.client_request_id,  m.command, m.key , m.value
		req_id = m.client_request_id
		shard_id = self.req_to_shard_map[req_id]
		if len(self.request_queue[shard_id]) > 0 and req_id == self.request_queue[shard_id][0][1]:
			self.request_queue[shard_id].pop(0)
			if m.command != 10:
				# if it is the add response in addshard the client_id would be none
				if m.client_id != None:
					v = self.client_port_info[m.client_id]
					send_message(v[0] , v[1] , encode_message(m))
			else:
				# send save command to new shard
				print ('get values for addshard %d' % (m.client_id))
				new_msg = self.request_queue[m.client_id][0][0]
				new_msg.key = m.key
				new_msg.value = m.value
				self.send_request(m.client_id , new_msg)
			del self.timeout_sheet[shard_id]
			if len(self.request_queue[shard_id]) > 0:
				new_msg = self.request_queue[shard_id][0][0]
				self.send_request(shard_id , new_msg)


	def send_request(self , shard_id , m):
		v = self.shard_port_info[shard_id][self.shard_view[shard_id]]
		print shard_id, m, v
		send_message(v[0], v[1], encode_message(m))
		self.timeout_sheet[shard_id] = time.time() + self.shard_next_timeout[shard_id]

	def handle_timeout(self , shard_id):
		self.shard_next_timeout[shard_id] *= 2
		self.shard_view[shard_id] += 1
		m = Message(mtype = 4 , sender_id = self.shard_view[shard_id])
		self.broadcast(shard_id , m)
		time.sleep(0.5)
		m = self.request_queue[shard_id][0]
		self.send_request(shard_id , m)

	def getTimeout(self):
		if len(self.timeout_sheet) == 0:
			nextTimeout = 10000
			timeout_shard = None
		else:
			timeout_shard = min(self.timeout_sheet , key = self.timeout_sheet.get)
			nextTimeout = self.timeout_sheet[timeout_shard] - time.time()
		return timeout_shard , nextTimeout

	def find_shard(self , val, n):
		best = -1
		best_dis = self.val2_64
		for i in range(n):
			dis = distance(val , self.shard_pos[i])
			if dis < best_dis:
				best = i
				best_dis = dis
		return best

	def broadcast(self , shard_id , m):
		for key in self.shard_port_info[shard_id].keys():
			v = self.shard_port_info[shard_id][key]
			send_message(v[0], v[1], m)
