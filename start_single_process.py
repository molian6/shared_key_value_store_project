import config, replica, os, shutil, client, master
import time, multiprocessing
import random
import sys

# Configure command line options
DEFAULT_NUM_FAILURES = 1
DEFAULT_NUM_CLIENTS = 1
DEFAULT_NUM_SHARDS = 1
DEBUG = True
SKIP = False
CLIENT_PORTS_INFO = "client_ports.txt"
SERVER_PORTS_INFO = "server_ports_0.txt"
MASTER_PORTS_INFO = "master_ports.txt"
START_TYPE = 1

CONTEXT_SETTINGS = dict(help_option_names=['-h', '--help'])

def read_ports_info(filename, n):
	s = {}
	with open(filename) as f:
		for i in range(n):
			l = f.readline()
			info = l.split(' ')
			s[int(info[0])] = [info[1], int(info[2])]
	return s

def find_args(cmd):
	for i,x in enumerate(sys.argv):
		if x == cmd:
			return sys.argv[i+1]
	return None

if __name__=="__main__":
	print 'begin'
	print 'start_type: 1-master, 2-replica, 3-client'
	if find_args('-start_type') != None:
		START_TYPE = int(find_args('-start_type'))
	if find_args('-num_failures') != None:
		DEFAULT_NUM_FAILURES = int(find_args('-num_failures'))
	if find_args('-num_clients') != None:
		DEFAULT_NUM_CLIENTS = int(find_args('-num_clients'))
	if find_args('-num_shards') != None:
		DEFAULT_NUM_SHARDS = int(find_args('-num_shards'))
	if find_args('-server_ports') != None:
		SERVER_PORTS_INFO = find_args('-server_ports')
	if find_args('-master_ports') != None:
		MASTER_PORTS_INFO = find_args('-master_ports')
	if find_args('-client_ports') != None:
		CLIENT_PORTS_INFO = find_args('-client_ports')
	if find_args('-index') != None:
		i = int(find_args('-index'))
	if find_args('-skip') != None:
		SKIP = int(find_args('-skip'))
		if SKIP == 1: SKIP = True
		else: SKIP = False

	client_ports_info = read_ports_info(CLIENT_PORTS_INFO , DEFAULT_NUM_CLIENTS)
	server_ports_info = read_ports_info(SERVER_PORTS_INFO , 2*DEFAULT_NUM_FAILURES+1)
	master_ports_info = read_ports_info(MASTER_PORTS_INFO , 1)

	# start master
	if START_TYPE == 1:
		shard_port_info = []
		for i in range(DEFAULT_NUM_SHARDS):
			shard_port_info.append(read_ports_info('server_ports_%d.txt' % (i) , 2*DEFAULT_NUM_FAILURES+1))
		p = multiprocessing.Process(target=master.Master, args = (shard_port_info , client_ports_info , master_ports_info[0])) #f, ID, port_info
		print "start master"
		p.start()

	# start replica
	elif START_TYPE == 2:
		p = multiprocessing.Process(target=replica.Replica, args = (DEFAULT_NUM_FAILURES , i , server_ports_info , master_ports_info[0] , DEBUG , SKIP)) #f, ID, port_info
		print "start replica %d" % (i)
		raw_input("Press Enter to continue...")
		p.start()

	# start client
	elif START_TYPE == 3:
		#e = multiprocessing.Event()
		#p = multiprocessing.Process(target = client.Client , args = (client_ports_info[i][0] , client_ports_info[i][1], i , master_ports_info[0] , e))
		client = client.Client(client_ports_info[i][0] , client_ports_info[i][1], i , master_ports_info[0])
		print "start client %d" % (i)
		while True:
			command = raw_input("Type command\n")
			if command == 'quit':
				client.response_listen_thread.terminate()
				print 'Quit client'
				break
			client.send_request(command)
			#print 'Client %d is waiting for responce of the previous message.'
